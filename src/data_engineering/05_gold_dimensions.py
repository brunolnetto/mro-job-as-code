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
# Contract: conformed dimensions for analytical joins. Surrogate keys are
# deterministic BIGINT hashes of durable source keys.

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_fq(gold_schema)}")

# COMMAND ----------
spark.sql(f"""
CREATE OR REPLACE TABLE {fq(gold_schema, "dim_supplier")}
USING DELTA AS
SELECT
    xxhash64(supplier_id) AS supplier_key,
    *
FROM {fq(silver_schema, "supplier")}
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {fq(gold_schema, "dim_material")}
USING DELTA AS
SELECT
    xxhash64(material_id) AS material_key,
    *
FROM {fq(silver_schema, "material")}
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {fq(gold_schema, "dim_warehouse")}
USING DELTA AS
SELECT
    xxhash64(warehouse_id) AS warehouse_key,
    *
FROM {fq(silver_schema, "warehouse")}
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {fq(gold_schema, "dim_cost_center")}
USING DELTA AS
SELECT
    xxhash64(cost_center_id) AS cost_center_key,
    *
FROM {fq(silver_schema, "cost_center")}
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {fq(gold_schema, "dim_asset")}
USING DELTA AS
SELECT
    xxhash64(asset_id) AS asset_key,
    *
FROM {fq(silver_schema, "asset")}
""")

# COMMAND ----------
# Date dimension follows the observed business-data horizon, not simulator state.

date_bounds = spark.sql(f"""
WITH horizon AS (
    SELECT data_horizon_at
    FROM {fq(bronze_schema, "ingestion_run")}
    WHERE completed_at IS NOT NULL
    ORDER BY completed_at DESC
    LIMIT 1
)
SELECT
    COALESCE(
      MIN(CAST(planned_at AS DATE)),
      CAST((SELECT data_horizon_at FROM horizon) AS DATE)
    ) AS min_date,
    CAST((SELECT data_horizon_at FROM horizon) AS DATE) AS max_date
FROM {fq(bronze_schema, "work_order_state_raw")}
""").first()

min_date = date_bounds["min_date"]
max_date = date_bounds["max_date"]

spark.sql(f"""
CREATE OR REPLACE TABLE {fq(gold_schema, "dim_date")}
USING DELTA AS
WITH dates AS (
  SELECT explode(sequence(DATE {sql_string(str(min_date))}, DATE {sql_string(str(max_date))}, INTERVAL 1 DAY)) AS full_date
)
SELECT
    CAST(date_format(full_date, 'yyyyMMdd') AS INT) AS date_key,
    full_date,
    year(full_date) AS year,
    quarter(full_date) AS quarter,
    month(full_date) AS month,
    date_format(full_date, 'MMMM') AS month_name,
    weekofyear(full_date) AS week_of_year,
    day(full_date) AS day_of_month,
    dayofweek(full_date) AS day_of_week,
    dayofweek(full_date) IN (1, 7) AS is_weekend
FROM dates
""")

print(f"Gold dimensions built for {min_date}..{max_date}")
