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
# Contract: normalize and enrich the maintenance domain without changing
# transactional grain.

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_fq(silver_schema)}")

sim = spark.sql(
    f"SELECT simulated_at, committed_tick FROM {fq(bronze_schema, 'sim_state_raw')} WHERE id = 'main'"
).first()
simulated_at = sim["simulated_at"]
committed_tick = int(sim["committed_tick"])

# COMMAND ----------
spark.sql(f"""
CREATE OR REPLACE TABLE {fq(silver_schema, "work_order_status_history")}
USING DELTA AS
SELECT
    entity_id AS work_order_id,
    from_state,
    to_state,
    reason,
    occurred_at,
    simulation_tick
FROM {fq(bronze_schema, "entity_state_transition_raw")}
WHERE entity_type = 'work_order'
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {fq(silver_schema, "work_order")}
USING DELTA AS
WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY valid_from_tick DESC
        ) AS rn
    FROM {fq(bronze_schema, "work_order_state_raw")}
    WHERE valid_from_tick <= {committed_tick}
)
SELECT
    w.id AS work_order_id,
    w.order_no,
    w.asset_id,
    a.description AS asset_description,
    a.asset_type,
    a.criticality AS asset_criticality,
    w.maintenance_type,
    w.priority,
    w.status,
    w.cost_center_id,
    a.cost_center_name,
    w.warehouse_id,
    a.warehouse_name,
    w.planned_at,
    w.release_after,
    w.released_at,
    w.started_at,
    w.complete_after,
    w.completed_at,
    w.close_after,
    w.closed_at,
    CASE WHEN w.released_at IS NOT NULL
         THEN timestampdiff(SECOND, w.planned_at, w.released_at) / 3600.0 END
         AS planning_hours,
    CASE WHEN w.released_at IS NOT NULL AND w.started_at IS NOT NULL
         THEN timestampdiff(SECOND, w.released_at, w.started_at) / 3600.0 END
         AS material_wait_hours,
    CASE WHEN w.started_at IS NOT NULL AND w.completed_at IS NOT NULL
         THEN timestampdiff(SECOND, w.started_at, w.completed_at) / 3600.0 END
         AS execution_hours,
    CASE WHEN w.completed_at IS NOT NULL AND w.closed_at IS NOT NULL
         THEN timestampdiff(SECOND, w.completed_at, w.closed_at) / 3600.0 END
         AS closure_hours,
    CASE WHEN w.closed_at IS NOT NULL
         THEN timestampdiff(SECOND, w.planned_at, w.closed_at) / 3600.0
         ELSE timestampdiff(SECOND, w.planned_at, TIMESTAMP {sql_string(str(simulated_at))}) / 3600.0
    END AS elapsed_cycle_hours,
    w.valid_from_tick
FROM ranked w
JOIN {fq(silver_schema, "asset")} a
  ON a.asset_id = w.asset_id
WHERE w.rn = 1
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {fq(silver_schema, "work_order_material")}
USING DELTA AS
WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY valid_from_tick DESC
        ) AS rn
    FROM {fq(bronze_schema, "work_order_material_state_raw")}
    WHERE valid_from_tick <= {committed_tick}
)
SELECT
    wom.id AS work_order_material_id,
    wom.work_order_id,
    wom.material_id,
    m.description AS material_description,
    m.category AS material_category,
    m.uom,
    wom.qty_required,
    wom.qty_issued,
    GREATEST(wom.qty_required - wom.qty_issued, 0.0) AS open_qty,
    CASE
      WHEN wom.qty_required = 0 THEN 1.0
      ELSE LEAST(wom.qty_issued / wom.qty_required, 1.0)
    END AS fulfillment_ratio,
    CAST(wom.qty_issued * m.unit_cost AS DECIMAL(18,2)) AS estimated_issued_cost,
    wom.status,
    wom.valid_from_tick
FROM ranked wom
JOIN {fq(silver_schema, "material")} m
  ON m.material_id = wom.material_id
WHERE wom.rn = 1
""")

# COMMAND ----------
spark.sql(f"""
CREATE OR REPLACE TABLE {fq(silver_schema, "work_order_lifecycle")}
USING DELTA AS
WITH material_agg AS (
    SELECT
        work_order_id,
        COUNT(*) AS material_line_count,
        SUM(CASE WHEN status = 'FULFILLED' THEN 1 ELSE 0 END) AS fulfilled_material_line_count,
        CAST(SUM(COALESCE(estimated_issued_cost, 0)) AS DECIMAL(18,2)) AS estimated_material_cost
    FROM {fq(silver_schema, "work_order_material")}
    GROUP BY work_order_id
),
status_agg AS (
    SELECT
        work_order_id,
        MAX(CASE WHEN to_state = 'WAITING_MATERIAL' THEN 1 ELSE 0 END) AS waited_for_material
    FROM {fq(silver_schema, "work_order_status_history")}
    GROUP BY work_order_id
)
SELECT
    wo.work_order_id,
    wo.order_no,
    wo.asset_id,
    wo.asset_type,
    wo.maintenance_type,
    wo.priority,
    wo.status,
    wo.planned_at,
    wo.released_at,
    wo.started_at,
    wo.completed_at,
    wo.closed_at,
    wo.planning_hours,
    wo.material_wait_hours,
    wo.execution_hours,
    wo.closure_hours,
    wo.elapsed_cycle_hours,
    COALESCE(ma.material_line_count, 0) AS material_line_count,
    COALESCE(ma.fulfilled_material_line_count, 0) AS fulfilled_material_line_count,
    COALESCE(ma.estimated_material_cost, CAST(0 AS DECIMAL(18,2))) AS estimated_material_cost,
    COALESCE(sa.waited_for_material, 0) AS waited_for_material
FROM {fq(silver_schema, "work_order")} wo
LEFT JOIN material_agg ma
  ON ma.work_order_id = wo.work_order_id
LEFT JOIN status_agg sa
  ON sa.work_order_id = wo.work_order_id
""")

print(f"Silver MRO built at committed_tick={committed_tick}")
