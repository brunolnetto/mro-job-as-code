# Databricks notebook source
# MRO analytical pipeline notebook.
# This file is intended to be imported as a Databricks notebook source file.

# COMMAND ----------

import json

def _widget(name: str, default: str) -> str:
    try:
        return dbutils.widgets.get(name)
    except Exception:
        dbutils.widgets.text(name, default)
        return dbutils.widgets.get(name)

catalog = _widget("catalog", "mro-data")
source_schema = _widget("source_schema", "mro_sim")
bronze_schema = _widget("bronze_schema", "bronze")
silver_schema = _widget("silver_schema", "silver")
gold_schema = _widget("gold_schema", "gold")
semantic_schema = _widget("semantic_schema", "semantic")

def q(identifier: str) -> str:
    if "`" in identifier:
        raise ValueError(f"Backticks are not allowed in identifier: {identifier!r}")
    return f"`{identifier}`"

def fq(schema: str, object_name: str) -> str:
    return f"{q(catalog)}.{q(schema)}.{q(object_name)}"

def schema_fq(schema: str) -> str:
    return f"{q(catalog)}.{q(schema)}"

def sql_string(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


# COMMAND ----------
# Contract
#
# INPUT:
#   <catalog>.<source_schema>  -- synthetic operational source
#
# OUTPUT:
#   <catalog>.<bronze_schema>  -- source-faithful Delta tables
#
# RESPONSIBILITY:
#   * preserve source grain and source history
#   * ingest only committed simulation ticks
#   * add ingestion metadata
#   * do not perform business enrichment

from datetime import datetime, timezone
from uuid import uuid4
from pyspark.sql import functions as F

run_id = str(uuid4())

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_fq(bronze_schema)}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {fq(bronze_schema, "pipeline_watermark")} (
    source_table STRING NOT NULL,
    last_tick BIGINT NOT NULL,
    updated_at TIMESTAMP NOT NULL
) USING DELTA
""")

state = spark.sql(
    f"SELECT simulated_at, committed_tick FROM {fq(source_schema, 'sim_state')} WHERE id = 'main'"
).first()
if state is None:
    raise RuntimeError(
        f"{fq(source_schema, 'sim_state')} is not initialized. "
        "Run the simulator/bootstrap task first."
    )

committed_tick = int(state["committed_tick"])
simulated_at = state["simulated_at"]

print(f"Bronze ingestion run_id={run_id}")
print(f"Source committed_tick={committed_tick}, simulated_at={simulated_at}")

# COMMAND ----------

def table_exists(schema: str, table: str) -> bool:
    rows = spark.sql(f"SHOW TABLES IN {schema_fq(schema)} LIKE '{table}'").collect()
    return any(r["tableName"] == table for r in rows)

def get_watermark(source_table: str) -> int:
    row = spark.sql(
        f"""
        SELECT last_tick
        FROM {fq(bronze_schema, "pipeline_watermark")}
        WHERE source_table = {sql_string(source_table)}
        """
    ).first()
    return int(row["last_tick"]) if row else -1

def set_watermark(source_table: str, tick: int) -> None:
    spark.sql(
        f"""
        MERGE INTO {fq(bronze_schema, "pipeline_watermark")} t
        USING (
            SELECT
                {sql_string(source_table)} AS source_table,
                CAST({tick} AS BIGINT) AS last_tick,
                current_timestamp() AS updated_at
        ) s
        ON t.source_table = s.source_table
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )

def merge_df(df, target_table: str, keys: list[str]) -> int:
    count = df.count()
    if count == 0:
        return 0

    temp = f"_bronze_{target_table}_{uuid4().hex}"
    df.createOrReplaceTempView(temp)

    if not table_exists(bronze_schema, target_table):
        spark.sql(
            f"""
            CREATE TABLE {fq(bronze_schema, target_table)}
            USING DELTA
            AS SELECT * FROM `{temp}`
            """
        )
    else:
        on = " AND ".join(f"t.{q(k)} = s.{q(k)}" for k in keys)
        spark.sql(
            f"""
            MERGE INTO {fq(bronze_schema, target_table)} t
            USING `{temp}` s
            ON {on}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """
        )

    spark.catalog.dropTempView(temp)
    return count

def add_metadata(df, source_table: str):
    return (
        df
        .withColumn("_bronze_ingested_at", F.current_timestamp())
        .withColumn("_bronze_run_id", F.lit(run_id))
        .withColumn("_source_table", F.lit(source_table))
        .withColumn("_source_committed_tick", F.lit(committed_tick).cast("long"))
    )

# COMMAND ----------
# Master/reference objects are small and mutable. Bronze preserves their current
# source representation and MERGEs by the source business key.

MASTER_TABLES = {
    "supplier": ["id"],
    "warehouse": ["id"],
    "cost_center": ["id"],
    "asset": ["id"],
    "material": ["id"],
    "asset_material_profile": ["asset_id", "material_id"],
    "sim_state": ["id"],
    "simulation_run": ["run_id"],
}

master_counts = {}

for source_table, keys in MASTER_TABLES.items():
    df = add_metadata(spark.table(fq(source_schema, source_table)), source_table)
    target = f"{source_table}_raw"
    master_counts[target] = merge_df(df, target, keys)

# COMMAND ----------
# Versioned state tables use valid_from_tick.

STATE_TABLES = {
    "stock_balance_state": ["material_id", "warehouse_id", "valid_from_tick"],
    "work_order_state": ["id", "valid_from_tick"],
    "work_order_material_state": ["id", "valid_from_tick"],
    "purchase_requisition_state": ["id", "valid_from_tick"],
    "purchase_order_state": ["id", "valid_from_tick"],
    "purchase_order_item_state": ["id", "valid_from_tick"],
    "fiscal_invoice_state": ["id", "valid_from_tick"],
    "accounts_payable_state": ["id", "valid_from_tick"],
}

state_counts = {}

for source_table, keys in STATE_TABLES.items():
    last_tick = get_watermark(source_table)
    df = (
        spark.table(fq(source_schema, source_table))
        .where(
            (F.col("valid_from_tick") > F.lit(last_tick))
            & (F.col("valid_from_tick") <= F.lit(committed_tick))
        )
    )
    df = add_metadata(df, source_table)
    target = f"{source_table}_raw"
    state_counts[target] = merge_df(df, target, keys)
    set_watermark(source_table, committed_tick)

# COMMAND ----------
# Append/event tables use simulation_tick.

APPEND_TABLES = {
    "purchase_requisition_item_store": ("purchase_requisition_item_raw", ["id"]),
    "goods_receipt_store": ("goods_receipt_raw", ["id"]),
    "goods_receipt_item_store": ("goods_receipt_item_raw", ["id"]),
    "inventory_movement_store": ("inventory_movement_raw", ["id"]),
    "fiscal_invoice_item_store": ("fiscal_invoice_item_raw", ["id"]),
    "payment_store": ("payment_raw", ["id"]),
    "entity_state_transition_store": ("entity_state_transition_raw", ["id"]),
    "event_log_store": ("event_log_raw", ["id"]),
}

append_counts = {}

for source_table, (target, keys) in APPEND_TABLES.items():
    last_tick = get_watermark(source_table)
    df = (
        spark.table(fq(source_schema, source_table))
        .where(
            (F.col("simulation_tick") > F.lit(last_tick))
            & (F.col("simulation_tick") <= F.lit(committed_tick))
        )
    )
    df = add_metadata(df, source_table)
    append_counts[target] = merge_df(df, target, keys)
    set_watermark(source_table, committed_tick)

# COMMAND ----------
summary = {
    "run_id": run_id,
    "committed_tick": committed_tick,
    "master_rows_seen": master_counts,
    "state_rows_ingested": state_counts,
    "event_rows_ingested": append_counts,
}

print(json.dumps(summary, indent=2, default=str))
dbutils.jobs.taskValues.set(key="bronze_committed_tick", value=committed_tick)
dbutils.jobs.taskValues.set(key="bronze_run_id", value=run_id)
