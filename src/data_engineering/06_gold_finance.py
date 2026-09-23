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
CREATE OR REPLACE TABLE {fq(gold_schema, "fact_fiscal_invoice")}
USING DELTA AS
SELECT
    fi.fiscal_invoice_id,
    fi.invoice_no,
    ds.supplier_key,
    fi.purchase_order_id,
    fi.goods_receipt_id,
    CAST(date_format(CAST(fi.received_at AS DATE), 'yyyyMMdd') AS INT) AS received_date_key,
    fi.status,
    fi.subtotal,
    fi.tax_total,
    fi.total_amount,
    fi.price_variance_pct,
    fi.tax_issue,
    fi.was_blocked,
    fi.issued_at,
    fi.received_at,
    fi.blocked_until
FROM {fq(silver_schema, "fiscal_invoice")} fi
JOIN {fq(gold_schema, "dim_supplier")} ds
  ON ds.supplier_id = fi.supplier_id
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {fq(gold_schema, "fact_fiscal_invoice_item")}
USING DELTA AS
SELECT
    fii.fiscal_invoice_item_id,
    fii.fiscal_invoice_id,
    fi.invoice_no,
    ds.supplier_key,
    dm.material_key,
    CAST(date_format(CAST(fi.received_at AS DATE), 'yyyyMMdd') AS INT) AS received_date_key,
    fii.quantity,
    fii.unit_price,
    fii.merchandise_value,
    fii.icms_amount,
    fii.ipi_amount,
    fii.pis_amount,
    fii.cofins_amount
FROM {fq(silver_schema, "fiscal_invoice_item")} fii
JOIN {fq(silver_schema, "fiscal_invoice")} fi
  ON fi.fiscal_invoice_id = fii.fiscal_invoice_id
JOIN {fq(gold_schema, "dim_supplier")} ds
  ON ds.supplier_id = fi.supplier_id
JOIN {fq(gold_schema, "dim_material")} dm
  ON dm.material_id = fii.material_id
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {fq(gold_schema, "fact_accounts_payable")}
USING DELTA AS
SELECT
    ap.accounts_payable_id,
    ap.fiscal_invoice_id,
    ds.supplier_key,
    CAST(date_format(CAST(ap.posted_at AS DATE), 'yyyyMMdd') AS INT) AS posted_date_key,
    CAST(date_format(CAST(ap.due_at AS DATE), 'yyyyMMdd') AS INT) AS due_date_key,
    CASE WHEN ap.paid_at IS NOT NULL
         THEN CAST(date_format(CAST(ap.paid_at AS DATE), 'yyyyMMdd') AS INT) END
         AS paid_date_key,
    ap.status,
    ap.amount,
    ap.posted_at,
    ap.due_at,
    ap.scheduled_at,
    ap.payment_scheduled_for,
    ap.paid_at,
    ap.voided_at,
    ap.days_to_pay,
    ap.is_overdue,
    ap.days_past_due
FROM {fq(silver_schema, "accounts_payable")} ap
JOIN {fq(gold_schema, "dim_supplier")} ds
  ON ds.supplier_id = ap.supplier_id
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {fq(gold_schema, "fact_payment")}
USING DELTA AS
SELECT
    p.payment_id,
    p.payment_no,
    p.accounts_payable_id,
    ap.supplier_key,
    CAST(date_format(CAST(p.paid_at AS DATE), 'yyyyMMdd') AS INT) AS payment_date_key,
    p.amount,
    p.paid_at
FROM {fq(silver_schema, "payment")} p
JOIN {fq(gold_schema, "fact_accounts_payable")} ap
  ON ap.accounts_payable_id = p.accounts_payable_id
""")

print("Gold finance facts built")
