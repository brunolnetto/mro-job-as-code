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
# Contract: normalize inventory events and current inventory state.

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_fq(silver_schema)}")

sim = spark.sql(
    f"SELECT simulated_at, committed_tick FROM {fq(bronze_schema, 'sim_state_raw')} WHERE id = 'main'"
).first()
committed_tick = int(sim["committed_tick"])

# COMMAND ----------
spark.sql(f"""
CREATE OR REPLACE TABLE {fq(silver_schema, "inventory_movement")}
USING DELTA AS
SELECT
    i.id AS inventory_movement_id,
    i.material_id,
    m.description AS material_description,
    m.category AS material_category,
    i.warehouse_id,
    w.warehouse_name,
    i.movement_type,
    i.quantity,
    CASE
      WHEN i.quantity > 0 THEN 'IN'
      WHEN i.quantity < 0 THEN 'OUT'
      ELSE 'ZERO'
    END AS movement_direction,
    CAST(i.unit_cost_cents / 100.0 AS DECIMAL(18,2)) AS unit_cost,
    CAST(i.quantity * i.unit_cost_cents / 100.0 AS DECIMAL(18,2)) AS movement_value,
    i.reference_type,
    i.reference_id,
    i.occurred_at,
    i.simulation_tick
FROM {fq(bronze_schema, "inventory_movement_raw")} i
JOIN {fq(silver_schema, "material")} m
  ON m.material_id = i.material_id
JOIN {fq(silver_schema, "warehouse")} w
  ON w.warehouse_id = i.warehouse_id
WHERE i.simulation_tick <= {committed_tick}
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {fq(silver_schema, "stock_position")}
USING DELTA AS
WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY material_id, warehouse_id
            ORDER BY valid_from_tick DESC
        ) AS rn
    FROM {fq(bronze_schema, "stock_balance_state_raw")}
    WHERE valid_from_tick <= {committed_tick}
)
SELECT
    s.material_id,
    m.description AS material_description,
    m.category AS material_category,
    s.warehouse_id,
    w.warehouse_name,
    s.on_hand,
    s.reserved,
    s.on_hand - s.reserved AS available,
    m.reorder_point,
    m.reorder_qty,
    CAST(s.on_hand * m.unit_cost AS DECIMAL(18,2)) AS inventory_value,
    s.last_counted_at,
    s.valid_from_tick
FROM ranked s
JOIN {fq(silver_schema, "material")} m
  ON m.material_id = s.material_id
JOIN {fq(silver_schema, "warehouse")} w
  ON w.warehouse_id = s.warehouse_id
WHERE s.rn = 1
""")

# COMMAND ----------
violations = spark.sql(
    f"""
    SELECT COUNT(*) AS n
    FROM {fq(silver_schema, "stock_position")}
    WHERE on_hand < 0 OR reserved < 0
    """
).first()["n"]

if violations:
    raise RuntimeError(f"Negative inventory state detected: {violations} rows")

print(f"Silver stock built at committed_tick={committed_tick}")
