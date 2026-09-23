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
# Contract
#
# INPUT:  bronze master/reference tables
# OUTPUT: normalized/conformed Silver master data
#
# RESPONSIBILITY:
#   * typing and unit normalization
#   * source-key uniqueness
#   * reference integrity
#   * expose business-friendly numeric units without changing grain

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_fq(silver_schema)}")

# COMMAND ----------
spark.sql(f"""
CREATE OR REPLACE TABLE {fq(silver_schema, "supplier")}
USING DELTA AS
SELECT
    id AS supplier_id,
    name AS supplier_name,
    tax_id,
    CAST(lead_time_days AS INT) AS lead_time_days,
    CAST(payment_term_days AS INT) AS payment_term_days,
    CAST(delivery_reliability AS DECIMAL(9,6)) AS delivery_reliability,
    CAST(price_variability AS DECIMAL(9,6)) AS price_variability,
    active,
    _bronze_ingested_at AS source_ingested_at
FROM {fq(bronze_schema, "supplier_raw")}
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {fq(silver_schema, "warehouse")}
USING DELTA AS
SELECT
    id AS warehouse_id,
    name AS warehouse_name,
    active,
    _bronze_ingested_at AS source_ingested_at
FROM {fq(bronze_schema, "warehouse_raw")}
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {fq(silver_schema, "cost_center")}
USING DELTA AS
SELECT
    id AS cost_center_id,
    name AS cost_center_name,
    active,
    _bronze_ingested_at AS source_ingested_at
FROM {fq(bronze_schema, "cost_center_raw")}
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {fq(silver_schema, "material")}
USING DELTA AS
SELECT
    id AS material_id,
    description,
    category,
    uom,
    ncm,
    criticality,
    CAST(unit_cost_cents / 100.0 AS DECIMAL(18,2)) AS unit_cost,
    reorder_point,
    reorder_qty,
    preferred_supplier_id,
    CAST(icms_bps / 100.0 AS DECIMAL(9,4)) AS icms_pct,
    CAST(ipi_bps / 100.0 AS DECIMAL(9,4)) AS ipi_pct,
    CAST(pis_bps / 100.0 AS DECIMAL(9,4)) AS pis_pct,
    CAST(cofins_bps / 100.0 AS DECIMAL(9,4)) AS cofins_pct,
    active,
    _bronze_ingested_at AS source_ingested_at
FROM {fq(bronze_schema, "material_raw")}
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {fq(silver_schema, "asset")}
USING DELTA AS
SELECT
    a.id AS asset_id,
    a.description,
    a.asset_type,
    a.criticality,
    a.cost_center_id,
    cc.cost_center_name,
    a.warehouse_id,
    w.warehouse_name,
    a.active,
    a._bronze_ingested_at AS source_ingested_at
FROM {fq(bronze_schema, "asset_raw")} a
LEFT JOIN {fq(silver_schema, "cost_center")} cc
  ON cc.cost_center_id = a.cost_center_id
LEFT JOIN {fq(silver_schema, "warehouse")} w
  ON w.warehouse_id = a.warehouse_id
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {fq(silver_schema, "asset_material_profile")}
USING DELTA AS
SELECT
    p.asset_id,
    a.asset_type,
    p.material_id,
    m.category AS material_category,
    CAST(p.probability AS DECIMAL(9,6)) AS probability,
    p.min_qty,
    p.max_qty
FROM {fq(bronze_schema, "asset_material_profile_raw")} p
JOIN {fq(silver_schema, "asset")} a
  ON a.asset_id = p.asset_id
JOIN {fq(silver_schema, "material")} m
  ON m.material_id = p.material_id
""")

# COMMAND ----------
# Hard master-data integrity checks.

checks = {
    "duplicate_supplier": f"""
        SELECT COUNT(*) n FROM (
          SELECT supplier_id FROM {fq(silver_schema, "supplier")}
          GROUP BY supplier_id HAVING COUNT(*) > 1
        )
    """,
    "duplicate_material": f"""
        SELECT COUNT(*) n FROM (
          SELECT material_id FROM {fq(silver_schema, "material")}
          GROUP BY material_id HAVING COUNT(*) > 1
        )
    """,
    "material_without_supplier": f"""
        SELECT COUNT(*) n
        FROM {fq(silver_schema, "material")} m
        LEFT JOIN {fq(silver_schema, "supplier")} s
          ON s.supplier_id = m.preferred_supplier_id
        WHERE s.supplier_id IS NULL
    """,
    "asset_without_cost_center": f"""
        SELECT COUNT(*) n
        FROM {fq(silver_schema, "asset")}
        WHERE cost_center_name IS NULL
    """,
}

failures = {name: spark.sql(sql).first()["n"] for name, sql in checks.items()}
bad = {k: v for k, v in failures.items() if v}
print({"checks": failures})

if bad:
    raise RuntimeError(f"Silver master quality failure: {bad}")
