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
# Gold MRO facts: dimensional models at explicit grains.

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_fq(gold_schema)}")

# COMMAND ----------
spark.sql(f"""
CREATE OR REPLACE TABLE {fq(gold_schema, "fact_work_order")}
USING DELTA AS
SELECT
    wo.work_order_id,
    wo.order_no,
    da.asset_key,
    dcc.cost_center_key,
    dw.warehouse_key,
    CAST(date_format(CAST(wo.planned_at AS DATE), 'yyyyMMdd') AS INT) AS planned_date_key,
    CASE WHEN wo.completed_at IS NOT NULL
         THEN CAST(date_format(CAST(wo.completed_at AS DATE), 'yyyyMMdd') AS INT) END
         AS completed_date_key,
    CASE WHEN wo.closed_at IS NOT NULL
         THEN CAST(date_format(CAST(wo.closed_at AS DATE), 'yyyyMMdd') AS INT) END
         AS closed_date_key,
    CASE WHEN wo.cancelled_at IS NOT NULL
         THEN CAST(date_format(CAST(wo.cancelled_at AS DATE), 'yyyyMMdd') AS INT) END
         AS cancelled_date_key,
    wo.maintenance_type,
    wo.priority,
    wo.status,
    wo.planned_at,
    wo.released_at,
    wo.started_at,
    wo.completed_at,
    wo.closed_at,
    wo.cancelled_at,
    wo.planning_hours,
    wo.material_wait_hours,
    wo.execution_hours,
    wo.closure_hours,
    wo.elapsed_cycle_hours,
    wo.material_line_count,
    wo.fulfilled_material_line_count,
    wo.estimated_material_cost,
    wo.waited_for_material
FROM {fq(silver_schema, "work_order_lifecycle")} wo
JOIN {fq(silver_schema, "work_order")} wod
  ON wod.work_order_id = wo.work_order_id
JOIN {fq(gold_schema, "dim_asset")} da
  ON da.asset_id = wo.asset_id
JOIN {fq(gold_schema, "dim_cost_center")} dcc
  ON dcc.cost_center_id = wod.cost_center_id
JOIN {fq(gold_schema, "dim_warehouse")} dw
  ON dw.warehouse_id = wod.warehouse_id
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {fq(gold_schema, "fact_work_order_material")}
USING DELTA AS
SELECT
    wom.work_order_material_id,
    wom.work_order_id,
    dm.material_key,
    da.asset_key,
    dcc.cost_center_key,
    dw.warehouse_key,
    CAST(date_format(CAST(wo.planned_at AS DATE), 'yyyyMMdd') AS INT) AS planned_date_key,
    wo.maintenance_type,
    wo.priority,
    wo.status AS work_order_status,
    wom.qty_required,
    wom.qty_issued,
    wom.open_qty,
    wom.fulfillment_ratio,
    wom.estimated_issued_cost,
    wom.status
FROM {fq(silver_schema, "work_order_material")} wom
JOIN {fq(silver_schema, "work_order")} wo
  ON wo.work_order_id = wom.work_order_id
JOIN {fq(gold_schema, "dim_material")} dm
  ON dm.material_id = wom.material_id
JOIN {fq(gold_schema, "dim_asset")} da
  ON da.asset_id = wo.asset_id
JOIN {fq(gold_schema, "dim_cost_center")} dcc
  ON dcc.cost_center_id = wo.cost_center_id
JOIN {fq(gold_schema, "dim_warehouse")} dw
  ON dw.warehouse_id = wo.warehouse_id
""")

print("Gold MRO facts built")
