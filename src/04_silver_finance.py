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
# Contract: normalize fiscal/AP/payment data and derive finance/fiscal lifecycle
# attributes. Monetary values are exposed in currency units instead of cents.

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_fq(silver_schema)}")

sim = spark.sql(
    f"SELECT simulated_at, committed_tick FROM {fq(bronze_schema, 'sim_state_raw')} WHERE id = 'main'"
).first()
simulated_at = sim["simulated_at"]
committed_tick = int(sim["committed_tick"])

# COMMAND ----------
spark.sql(f"""
CREATE OR REPLACE TABLE {fq(silver_schema, "fiscal_invoice")}
USING DELTA AS
WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY id ORDER BY valid_from_tick DESC
    ) rn
    FROM {fq(bronze_schema, "fiscal_invoice_state_raw")}
    WHERE valid_from_tick <= {committed_tick}
),
blocked AS (
    SELECT entity_id AS invoice_id, 1 AS was_blocked
    FROM {fq(bronze_schema, "entity_state_transition_raw")}
    WHERE entity_type = 'fiscal_invoice'
      AND to_state = 'BLOCKED'
      AND simulation_tick <= {committed_tick}
    GROUP BY entity_id
)
SELECT
    fi.id AS fiscal_invoice_id,
    fi.invoice_no,
    fi.supplier_id,
    s.supplier_name,
    fi.purchase_order_id,
    fi.goods_receipt_id,
    fi.status,
    fi.issued_at,
    fi.received_at,
    fi.validate_after,
    fi.validation_complete_after,
    fi.blocked_until,
    CAST(fi.subtotal_cents / 100.0 AS DECIMAL(18,2)) AS subtotal,
    CAST(fi.tax_total_cents / 100.0 AS DECIMAL(18,2)) AS tax_total,
    CAST(fi.total_amount_cents / 100.0 AS DECIMAL(18,2)) AS total_amount,
    CAST(fi.price_variance_bps / 100.0 AS DECIMAL(9,4)) AS price_variance_pct,
    fi.tax_issue,
    COALESCE(b.was_blocked, 0) = 1 AS was_blocked,
    fi.valid_from_tick
FROM ranked fi
JOIN {fq(silver_schema, "supplier")} s
  ON s.supplier_id = fi.supplier_id
LEFT JOIN blocked b
  ON b.invoice_id = fi.id
WHERE fi.rn = 1
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {fq(silver_schema, "fiscal_invoice_item")}
USING DELTA AS
SELECT
    i.id AS fiscal_invoice_item_id,
    i.invoice_id AS fiscal_invoice_id,
    i.material_id,
    m.description AS material_description,
    i.quantity,
    CAST(i.unit_price_cents / 100.0 AS DECIMAL(18,2)) AS unit_price,
    CAST(i.quantity * i.unit_price_cents / 100.0 AS DECIMAL(18,2)) AS merchandise_value,
    CAST(i.icms_cents / 100.0 AS DECIMAL(18,2)) AS icms_amount,
    CAST(i.ipi_cents / 100.0 AS DECIMAL(18,2)) AS ipi_amount,
    CAST(i.pis_cents / 100.0 AS DECIMAL(18,2)) AS pis_amount,
    CAST(i.cofins_cents / 100.0 AS DECIMAL(18,2)) AS cofins_amount,
    i.simulation_tick
FROM {fq(bronze_schema, "fiscal_invoice_item_raw")} i
JOIN {fq(silver_schema, "material")} m
  ON m.material_id = i.material_id
WHERE i.simulation_tick <= {committed_tick}
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {fq(silver_schema, "accounts_payable")}
USING DELTA AS
WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY id ORDER BY valid_from_tick DESC
    ) rn
    FROM {fq(bronze_schema, "accounts_payable_state_raw")}
    WHERE valid_from_tick <= {committed_tick}
)
SELECT
    ap.id AS accounts_payable_id,
    ap.invoice_id AS fiscal_invoice_id,
    ap.supplier_id,
    s.supplier_name,
    ap.status,
    CAST(ap.amount_cents / 100.0 AS DECIMAL(18,2)) AS amount,
    ap.posted_at,
    ap.due_at,
    ap.scheduled_at,
    ap.payment_scheduled_for,
    ap.paid_at,
    CASE
      WHEN ap.paid_at IS NOT NULL
        THEN timestampdiff(DAY, ap.posted_at, ap.paid_at)
    END AS days_to_pay,
    CASE
      WHEN ap.status <> 'PAID' AND ap.due_at < TIMESTAMP {sql_string(str(simulated_at))}
        THEN TRUE
      ELSE FALSE
    END AS is_overdue,
    CASE
      WHEN ap.status <> 'PAID' AND ap.due_at < TIMESTAMP {sql_string(str(simulated_at))}
        THEN datediff(CAST(TIMESTAMP {sql_string(str(simulated_at))} AS DATE), CAST(ap.due_at AS DATE))
      ELSE 0
    END AS days_past_due,
    ap.valid_from_tick
FROM ranked ap
JOIN {fq(silver_schema, "supplier")} s
  ON s.supplier_id = ap.supplier_id
WHERE ap.rn = 1
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {fq(silver_schema, "payment")}
USING DELTA AS
SELECT
    p.id AS payment_id,
    p.payment_no,
    p.accounts_payable_id,
    CAST(p.amount_cents / 100.0 AS DECIMAL(18,2)) AS amount,
    p.paid_at,
    p.simulation_tick
FROM {fq(bronze_schema, "payment_raw")} p
WHERE p.simulation_tick <= {committed_tick}
""")

print(f"Silver finance built at committed_tick={committed_tick}")
