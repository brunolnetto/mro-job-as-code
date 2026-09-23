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
# Final hard quality gate.
#
# This notebook should be the last task in the DAG. It fails the job when a
# structural business invariant is violated.

checks = {}

def check(name: str, sql: str, expected: int = 0):
    actual = int(spark.sql(sql).first()[0])
    checks[name] = {"actual": actual, "expected": expected}
    return actual == expected

# COMMAND ----------
check(
    "negative_stock",
    f"""
    SELECT COUNT(*)
    FROM {fq(silver_schema, "stock_position")}
    WHERE on_hand < 0 OR reserved < 0
    """,
)

check(
    "work_order_material_overissue",
    f"""
    SELECT COUNT(*)
    FROM {fq(silver_schema, "work_order_material")}
    WHERE qty_issued > qty_required + 1e-9
    """,
)

check(
    "purchase_order_overreceipt",
    f"""
    SELECT COUNT(*)
    FROM {fq(silver_schema, "purchase_order_item")}
    WHERE received_qty > ordered_qty + 1e-9
    """,
)

check(
    "paid_ap_without_paid_at",
    f"""
    SELECT COUNT(*)
    FROM {fq(silver_schema, "accounts_payable")}
    WHERE status = 'PAID' AND paid_at IS NULL
    """,
)

check(
    "orphan_gold_work_order_asset",
    f"""
    SELECT COUNT(*)
    FROM {fq(gold_schema, "fact_work_order")} f
    LEFT JOIN {fq(gold_schema, "dim_asset")} d
      ON d.asset_key = f.asset_key
    WHERE d.asset_key IS NULL
    """,
)

check(
    "orphan_gold_inventory_material",
    f"""
    SELECT COUNT(*)
    FROM {fq(gold_schema, "fact_inventory_movement")} f
    LEFT JOIN {fq(gold_schema, "dim_material")} d
      ON d.material_key = f.material_key
    WHERE d.material_key IS NULL
    """,
)

check(
    "duplicate_gold_material_key",
    f"""
    SELECT COUNT(*)
    FROM (
      SELECT material_key
      FROM {fq(gold_schema, "dim_material")}
      GROUP BY material_key
      HAVING COUNT(*) > 1
    )
    """,
)

# Bronze watermarks may be behind only if a table has never existed/been
# processed. Once present, none may exceed the source committed tick.
check(
    "watermark_ahead_of_source",
    f"""
    SELECT COUNT(*)
    FROM {fq(bronze_schema, "pipeline_watermark")}
    WHERE last_tick > (
      SELECT committed_tick
      FROM {fq(bronze_schema, "sim_state_raw")}
      WHERE id = 'main'
    )
    """,
)

# COMMAND ----------
failures = {name: result for name, result in checks.items()
            if result["actual"] != result["expected"]}

print(json.dumps(checks, indent=2))

dbutils.jobs.taskValues.set(
    key="quality_status",
    value="PASS" if not failures else "FAIL",
)

if failures:
    raise RuntimeError(f"MRO analytical quality gate failed: {json.dumps(failures)}")

print("QUALITY GATE: PASS")
