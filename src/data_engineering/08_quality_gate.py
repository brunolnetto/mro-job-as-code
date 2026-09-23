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
reconciliation_failure_threshold = int(_widget("reconciliation_failure_threshold", "3"))

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

check(
    "cancelled_work_order_with_active_material_demand",
    f"""
    SELECT COUNT(*)
    FROM {fq(silver_schema, "work_order")} wo
    JOIN {fq(silver_schema, "work_order_material")} wom
      ON wom.work_order_id = wo.work_order_id
    WHERE wo.status = 'CANCELLED'
      AND wom.status NOT IN ('CANCELLED', 'FULFILLED')
    """,
)

check(
    "cancelled_po_with_non_cancelled_requisition",
    f"""
    SELECT COUNT(*)
    FROM {fq(silver_schema, "purchase_order")} po
    JOIN {fq(silver_schema, "purchase_requisition")} pr
      ON pr.purchase_requisition_id = po.purchase_requisition_id
    WHERE po.status = 'CANCELLED'
      AND pr.status <> 'CANCELLED'
    """,
)

check(
    "voided_ap_with_payment",
    f"""
    SELECT COUNT(*)
    FROM {fq(silver_schema, "accounts_payable")} ap
    JOIN {fq(silver_schema, "payment")} p
      ON p.accounts_payable_id = ap.accounts_payable_id
    WHERE ap.status = 'VOIDED'
    """,
)

check(
    "released_invoice_without_prior_block",
    f"""
    SELECT COUNT(*)
    FROM {fq(silver_schema, "fiscal_invoice")}
    WHERE status = 'RELEASED'
      AND was_blocked = FALSE
    """,
)

# Bronze owns source positions. A checkpoint may lag the source, but it may
# never be ahead of the current Delta history.
checkpoint_ahead = 0
for checkpoint in spark.table(fq(bronze_schema, "ingestion_checkpoint")).collect():
    source_table = checkpoint["source_table"]
    current = spark.sql(
        f"DESCRIBE HISTORY {fq(source_schema, source_table)}"
    ).selectExpr("MAX(version) AS max_version").first()["max_version"]
    if current is None or int(checkpoint["last_commit_version"]) > int(current):
        checkpoint_ahead += 1
checks["checkpoint_ahead_of_source"] = {"actual": checkpoint_ahead, "expected": 0}


# COMMAND ----------
# Cross-entity invariants can be transient because independent source tables
# can advance at different Delta commit positions. Only persistent mismatches
# become fatal.
ASYNC_RECONCILIATION_CHECKS = {
    "cancelled_work_order_with_active_material_demand",
    "cancelled_po_with_non_cancelled_requisition",
    "voided_ap_with_payment",
    "released_invoice_without_prior_block",
}

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {fq(bronze_schema, "reconciliation_check_state")} (
    check_name STRING NOT NULL,
    first_failed_at TIMESTAMP,
    last_failed_at TIMESTAMP,
    consecutive_failures INT NOT NULL,
    last_actual BIGINT NOT NULL,
    updated_at TIMESTAMP NOT NULL
) USING DELTA
""")

for check_name in ASYNC_RECONCILIATION_CHECKS:
    actual = int(checks[check_name]["actual"])
    previous = spark.sql(f"""
        SELECT consecutive_failures, first_failed_at
        FROM {fq(bronze_schema, "reconciliation_check_state")}
        WHERE check_name = {sql_string(check_name)}
    """).first()

    if actual == 0:
        consecutive = 0
        first_failed_sql = "CAST(NULL AS TIMESTAMP)"
        last_failed_sql = "CAST(NULL AS TIMESTAMP)"
    else:
        consecutive = (int(previous["consecutive_failures"]) if previous else 0) + 1
        first_failed_sql = (
            "current_timestamp()"
            if previous is None or previous["first_failed_at"] is None
            else f"TIMESTAMP {sql_string(str(previous['first_failed_at']))}"
        )
        last_failed_sql = "current_timestamp()"

    spark.sql(f"""
        MERGE INTO {fq(bronze_schema, "reconciliation_check_state")} t
        USING (SELECT
            {sql_string(check_name)} AS check_name,
            {first_failed_sql} AS first_failed_at,
            {last_failed_sql} AS last_failed_at,
            {consecutive} AS consecutive_failures,
            CAST({actual} AS BIGINT) AS last_actual,
            current_timestamp() AS updated_at
        ) s
        ON t.check_name=s.check_name
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    checks[check_name]["raw_actual"] = actual
    checks[check_name]["consecutive_failures"] = consecutive
    checks[check_name]["threshold"] = reconciliation_failure_threshold
    checks[check_name]["actual"] = actual if consecutive >= reconciliation_failure_threshold else 0

# COMMAND ----------
# Semantic coverage contract: every Gold fact must have one primary metric view.

EXPECTED_FACT_TO_CUBE = {
    "fact_work_order": "maintenance_metrics",
    "fact_work_order_material": "maintenance_material_metrics",
    "fact_inventory_movement": "inventory_metrics",
    "fact_stock_position": "inventory_position_metrics",
    "fact_purchase_order_item": "procurement_metrics",
    "fact_goods_receipt_item": "receiving_metrics",
    "fact_fiscal_invoice": "invoice_metrics",
    "fact_fiscal_invoice_item": "fiscal_item_metrics",
    "fact_accounts_payable": "payables_metrics",
    "fact_payment": "payment_metrics",
}

registry_rows = spark.table(fq(semantic_schema, "cube_registry")).collect()
registry = {
    row["fact_table"]: row["cube_name"]
    for row in registry_rows
}

missing_fact_mappings = sorted(set(EXPECTED_FACT_TO_CUBE) - set(registry))
unexpected_fact_mappings = sorted(set(registry) - set(EXPECTED_FACT_TO_CUBE))
incorrect_cube_mappings = sorted(
    fact
    for fact, cube in EXPECTED_FACT_TO_CUBE.items()
    if registry.get(fact) != cube
)

checks["semantic_fact_coverage"] = {
    "actual": (
        len(missing_fact_mappings)
        + len(unexpected_fact_mappings)
        + len(incorrect_cube_mappings)
    ),
    "expected": 0,
}

view_rows = spark.sql(f"SHOW VIEWS IN {schema_fq(semantic_schema)}").collect()
view_names = set()
for row in view_rows:
    values = row.asDict()
    name = values.get("viewName") or values.get("view_name") or values.get("name")
    if name:
        view_names.add(name)

missing_cubes = sorted(set(EXPECTED_FACT_TO_CUBE.values()) - view_names)
checks["semantic_cube_coverage"] = {
    "actual": len(missing_cubes),
    "expected": 0,
}

if missing_fact_mappings:
    print(f"Missing fact mappings: {missing_fact_mappings}")
if unexpected_fact_mappings:
    print(f"Unexpected fact mappings: {unexpected_fact_mappings}")
if incorrect_cube_mappings:
    print(f"Incorrect fact->cube mappings: {incorrect_cube_mappings}")
if missing_cubes:
    print(f"Missing metric views: {missing_cubes}")

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
