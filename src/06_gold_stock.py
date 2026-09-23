# Databricks notebook source
# MRO analytical pipeline notebook.
# This file is intended to be imported as a Databricks notebook source file.

# COMMAND ----------

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
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_fq(gold_schema)}")

# COMMAND ----------
spark.sql(f"""
CREATE OR REPLACE TABLE {fq(gold_schema, "fact_inventory_movement")}
USING DELTA AS
SELECT
    i.inventory_movement_id,
    dm.material_key,
    dw.warehouse_key,
    CAST(date_format(CAST(i.occurred_at AS DATE), 'yyyyMMdd') AS INT) AS movement_date_key,
    i.movement_type,
    i.movement_direction,
    i.quantity,
    i.unit_cost,
    i.movement_value,
    i.reference_type,
    i.reference_id,
    i.occurred_at,
    i.simulation_tick
FROM {fq(silver_schema, "inventory_movement")} i
JOIN {fq(gold_schema, "dim_material")} dm
  ON dm.material_id = i.material_id
JOIN {fq(gold_schema, "dim_warehouse")} dw
  ON dw.warehouse_id = i.warehouse_id
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {fq(gold_schema, "fact_stock_position")}
USING DELTA AS
SELECT
    dm.material_key,
    dw.warehouse_key,
    CAST(
      (SELECT committed_tick FROM {fq(bronze_schema, "sim_state_raw")} WHERE id='main')
      AS BIGINT
    ) AS snapshot_tick,
    CAST(
      date_format(
        CAST((SELECT simulated_at FROM {fq(bronze_schema, "sim_state_raw")} WHERE id='main') AS DATE),
        'yyyyMMdd'
      ) AS INT
    ) AS snapshot_date_key,
    s.on_hand,
    s.reserved,
    s.available,
    s.reorder_point,
    s.reorder_qty,
    s.inventory_value,
    s.last_counted_at
FROM {fq(silver_schema, "stock_position")} s
JOIN {fq(gold_schema, "dim_material")} dm
  ON dm.material_id = s.material_id
JOIN {fq(gold_schema, "dim_warehouse")} dw
  ON dw.warehouse_id = s.warehouse_id
""")

print("Gold inventory facts built")
