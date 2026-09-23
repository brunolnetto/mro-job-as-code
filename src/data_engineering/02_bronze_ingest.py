# Databricks notebook source
# MRO analytical pipeline notebook.

# COMMAND ----------

import json
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from pyspark.sql import functions as F
from pyspark.sql.window import Window


def _widget(name: str, default: str) -> str:
    try:
        return dbutils.widgets.get(name)
    except Exception:
        dbutils.widgets.text(name, default)
        return dbutils.widgets.get(name)


catalog = _widget("catalog", "mro-data")
source_schema = _widget("source_schema", "mro_sim")
bronze_schema = _widget("bronze_schema", "bronze")
source_stability_lag_minutes = int(_widget("source_stability_lag_minutes", "5"))


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
# Analytics does not consume the simulator's logical tick. Bronze owns one
# independent Delta commit checkpoint per operational source table.

run_id = str(uuid4())
started_at = datetime.now(timezone.utc)
stable_cutoff_at = started_at - timedelta(minutes=source_stability_lag_minutes)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_fq(bronze_schema)}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {fq(bronze_schema, 'ingestion_checkpoint')} (
    source_table STRING NOT NULL,
    last_commit_version BIGINT NOT NULL,
    last_commit_timestamp TIMESTAMP,
    updated_at TIMESTAMP NOT NULL
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {fq(bronze_schema, 'ingestion_run')} (
    run_id STRING NOT NULL,
    started_at TIMESTAMP NOT NULL,
    stable_cutoff_at TIMESTAMP NOT NULL,
    data_horizon_at TIMESTAMP,
    completed_at TIMESTAMP,
    source_table_count INT NOT NULL,
    rows_ingested BIGINT NOT NULL
) USING DELTA
""")

MASTER_TABLES = {
    "supplier": ["id"],
    "warehouse": ["id"],
    "cost_center": ["id"],
    "asset": ["id"],
    "material": ["id"],
    "asset_material_profile": ["asset_id", "material_id"],
}

STATE_TABLES = {
    "stock_balance_state": (["material_id", "warehouse_id"], "valid_from_tick"),
    "work_order_state": (["id"], "valid_from_tick"),
    "work_order_material_state": (["id"], "valid_from_tick"),
    "purchase_requisition_state": (["id"], "valid_from_tick"),
    "purchase_order_state": (["id"], "valid_from_tick"),
    "purchase_order_item_state": (["id"], "valid_from_tick"),
    "fiscal_invoice_state": (["id"], "valid_from_tick"),
    "accounts_payable_state": (["id"], "valid_from_tick"),
}

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

SOURCE_TABLES = list(MASTER_TABLES) + list(STATE_TABLES) + list(APPEND_TABLES)
PRIVATE_SOURCE_COLUMNS = {"valid_from_tick", "simulation_tick"}


def table_exists(schema: str, table: str) -> bool:
    rows = spark.sql(f"SHOW TABLES IN {schema_fq(schema)} LIKE '{table}'").collect()
    return any(r["tableName"] == table for r in rows)


def get_checkpoint(source_table: str):
    return spark.sql(f"""
        SELECT last_commit_version, last_commit_timestamp
        FROM {fq(bronze_schema, 'ingestion_checkpoint')}
        WHERE source_table = {sql_string(source_table)}
    """).first()


def set_checkpoint(source_table: str, version: int, commit_timestamp) -> None:
    ts_sql = "CAST(NULL AS TIMESTAMP)" if commit_timestamp is None else f"TIMESTAMP {sql_string(str(commit_timestamp))}"
    spark.sql(f"""
        MERGE INTO {fq(bronze_schema, 'ingestion_checkpoint')} t
        USING (
            SELECT {sql_string(source_table)} AS source_table,
                   CAST({int(version)} AS BIGINT) AS last_commit_version,
                   {ts_sql} AS last_commit_timestamp,
                   current_timestamp() AS updated_at
        ) s
        ON t.source_table = s.source_table
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def stable_end(source_table: str):
    history = spark.sql(f"DESCRIBE HISTORY {fq(source_schema, source_table)}")
    row = (history
        .where(F.col("timestamp") <= F.lit(stable_cutoff_at))
        .orderBy(F.col("version").desc())
        .select("version", "timestamp")
        .first())
    return None if row is None else (int(row["version"]), row["timestamp"])


def source_snapshot(source_table: str, version: int):
    return spark.read.option("versionAsOf", int(version)).table(fq(source_schema, source_table))


def source_changes(source_table: str, start_version: int, end_version: int):
    return (spark.read
        .option("readChangeFeed", "true")
        .option("startingVersion", int(start_version))
        .option("endingVersion", int(end_version))
        .table(fq(source_schema, source_table)))


def publicize(df, source_table: str, *, snapshot_version=None, snapshot_timestamp=None):
    payload_cols = [
        c for c in df.columns
        if c not in PRIVATE_SOURCE_COLUMNS
        and c not in {"_change_type", "_commit_version", "_commit_timestamp"}
    ]
    record_hash = F.sha2(
        F.concat_ws("||", *[
            F.coalesce(F.col(c).cast("string"), F.lit("∅")) for c in sorted(payload_cols)
        ]),
        256,
    )
    out = df.withColumn("_source_record_hash", record_hash)
    for private_col in PRIVATE_SOURCE_COLUMNS:
        if private_col in out.columns:
            out = out.drop(private_col)

    if snapshot_version is not None:
        out = (out
            .withColumn("_source_change_type", F.lit("snapshot"))
            .withColumn("_source_commit_version", F.lit(int(snapshot_version)).cast("long"))
            .withColumn("_source_commit_timestamp", F.lit(snapshot_timestamp).cast("timestamp")))
    else:
        out = (out
            .withColumnRenamed("_change_type", "_source_change_type")
            .withColumnRenamed("_commit_version", "_source_commit_version")
            .withColumnRenamed("_commit_timestamp", "_source_commit_timestamp"))

    return (out
        .withColumn("_bronze_ingested_at", F.current_timestamp())
        .withColumn("_bronze_run_id", F.lit(run_id))
        .withColumn("_source_table", F.lit(source_table)))


def latest_per_key(df, keys):
    w = Window.partitionBy(*keys).orderBy(
        F.col("_source_commit_version").desc(),
        F.col("_source_commit_timestamp").desc(),
        F.col("_source_record_hash").desc(),
    )
    return df.withColumn("__rn", F.row_number().over(w)).where("__rn = 1").drop("__rn")


def merge_df(df, target_table: str, keys) -> int:
    count = df.count()
    if count == 0:
        return 0
    temp = f"_bronze_{target_table}_{uuid4().hex}"
    df.createOrReplaceTempView(temp)
    if not table_exists(bronze_schema, target_table):
        spark.sql(f"CREATE TABLE {fq(bronze_schema, target_table)} USING DELTA AS SELECT * FROM `{temp}`")
    else:
        on = " AND ".join(f"t.{q(k)} = s.{q(k)}" for k in keys)
        spark.sql(f"""
            MERGE INTO {fq(bronze_schema, target_table)} t
            USING `{temp}` s
            ON {on}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
    spark.catalog.dropTempView(temp)
    return count


def reject_source_deletes(df, source_table: str):
    if "_change_type" in df.columns and df.where(F.col("_change_type") == "delete").limit(1).count():
        raise RuntimeError(
            f"Source table {source_table!r} emitted DELETE changes. "
            "This source contract currently requires explicit tombstone handling."
        )


counts, advanced = {}, {}
for source_table in SOURCE_TABLES:
    end = stable_end(source_table)
    if end is None:
        counts[source_table] = 0
        continue
    end_version, end_timestamp = end
    checkpoint = get_checkpoint(source_table)

    if checkpoint is not None and int(checkpoint["last_commit_version"]) > end_version:
        raise RuntimeError(
            f"Checkpoint for {source_table} is ahead of current source history; "
            "the source was probably reset/replaced. Reset Bronze checkpoints explicitly."
        )

    target = APPEND_TABLES[source_table][0] if source_table in APPEND_TABLES else f"{source_table}_raw"

    if checkpoint is None:
        df = source_snapshot(source_table, end_version)
        if source_table in STATE_TABLES:
            business_keys, private_sequence = STATE_TABLES[source_table]
            w = Window.partitionBy(*business_keys).orderBy(F.col(private_sequence).desc())
            df = df.withColumn("__rn", F.row_number().over(w)).where("__rn = 1").drop("__rn")
        df = publicize(df, source_table, snapshot_version=end_version, snapshot_timestamp=end_timestamp)
        if source_table in MASTER_TABLES:
            df = latest_per_key(df, MASTER_TABLES[source_table])
            keys = MASTER_TABLES[source_table]
        elif source_table in STATE_TABLES:
            keys = ["_source_record_hash"]
        else:
            keys = APPEND_TABLES[source_table][1]
        counts[source_table] = merge_df(df, target, keys)
        set_checkpoint(source_table, end_version, end_timestamp)
        advanced[source_table] = end_version
        continue

    start_version = int(checkpoint["last_commit_version"]) + 1
    if start_version > end_version:
        counts[source_table] = 0
        advanced[source_table] = int(checkpoint["last_commit_version"])
        continue

    df = source_changes(source_table, start_version, end_version)
    reject_source_deletes(df, source_table)
    df = df.where(F.col("_change_type").isin("insert", "update_postimage"))
    df = publicize(df, source_table)

    if source_table in MASTER_TABLES:
        df = latest_per_key(df, MASTER_TABLES[source_table])
        keys = MASTER_TABLES[source_table]
    elif source_table in STATE_TABLES:
        keys = ["_source_record_hash"]
    else:
        keys = APPEND_TABLES[source_table][1]

    counts[source_table] = merge_df(df, target, keys)
    set_checkpoint(source_table, end_version, end_timestamp)
    advanced[source_table] = end_version

# observed business-data horizon; never used as a completeness watermark
if table_exists(bronze_schema, "event_log_raw"):
    row = spark.sql(f"SELECT MAX(occurred_at) AS h FROM {fq(bronze_schema, 'event_log_raw')}").first()
    data_horizon_at = row["h"] if row and row["h"] is not None else stable_cutoff_at
else:
    data_horizon_at = stable_cutoff_at

rows_ingested = int(sum(counts.values()))
spark.sql(f"""
INSERT INTO {fq(bronze_schema, 'ingestion_run')}
VALUES (
    {sql_string(run_id)},
    TIMESTAMP {sql_string(str(started_at))},
    TIMESTAMP {sql_string(str(stable_cutoff_at))},
    TIMESTAMP {sql_string(str(data_horizon_at))},
    current_timestamp(),
    {len(SOURCE_TABLES)},
    {rows_ingested}
)
""")

summary = {
    "run_id": run_id,
    "stable_cutoff_at": stable_cutoff_at.isoformat(),
    "data_horizon_at": str(data_horizon_at),
    "rows_ingested": rows_ingested,
    "source_commit_versions": advanced,
}
print(json.dumps(summary, indent=2, default=str))
dbutils.jobs.taskValues.set(key="bronze_run_id", value=run_id)
dbutils.jobs.taskValues.set(key="bronze_data_horizon_at", value=str(data_horizon_at))
