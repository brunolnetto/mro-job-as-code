# Databricks notebook source
# Incremental finance facts with SCD2 keys fixed at first insert.
# COMMAND ----------
def _widget(n,d):
    try:return dbutils.widgets.get(n)
    except Exception: dbutils.widgets.text(n,d); return dbutils.widgets.get(n)
catalog=_widget('catalog','mro-data'); silver_schema=_widget('silver_schema','silver'); gold_schema=_widget('gold_schema','gold')
def q(x):return f'`{x}`'
def fq(s,o):return f'{q(catalog)}.{q(s)}.{q(o)}'

def merge(target,pk,sql,preserve):
    view=f'_src_{target}'; spark.sql(f"CREATE OR REPLACE TEMP VIEW {q(view)} AS {sql}")
    if not spark.catalog.tableExists(fq(gold_schema,target)):
        spark.sql(f"CREATE TABLE {fq(gold_schema,target)} USING DELTA AS SELECT * FROM {q(view)}")
    else:
        cols=[r['col_name'] for r in spark.sql(f'DESCRIBE {fq(gold_schema,target)}').collect() if r['col_name'] and not r['col_name'].startswith('#')]
        upd=', '.join(f't.{q(c)}=s.{q(c)}' for c in cols if c not in set(preserve+[pk]))
        clause=f'WHEN MATCHED THEN UPDATE SET {upd} ' if upd else ''
        spark.sql(f"MERGE INTO {fq(gold_schema,target)} t USING {q(view)} s ON t.{q(pk)}=s.{q(pk)} {clause}WHEN NOT MATCHED THEN INSERT *")

merge('fact_fiscal_invoice','fiscal_invoice_id',f"""
SELECT fi.fiscal_invoice_id,fi.invoice_no,ds.supplier_key,fi.purchase_order_id,fi.goods_receipt_id,
 CAST(date_format(CAST(fi.received_at AS DATE),'yyyyMMdd') AS INT) received_date_key,
 fi.status,fi.subtotal,fi.tax_total,fi.total_amount,fi.price_variance_pct,fi.tax_issue,fi.was_blocked,fi.issued_at,fi.received_at,fi.blocked_until
FROM {fq(silver_schema,'fiscal_invoice')} fi
JOIN {fq(gold_schema,'dim_supplier')} ds ON ds.supplier_id=fi.supplier_id AND ds.is_current=TRUE
""", ['supplier_key'])

merge('fact_fiscal_invoice_item','fiscal_invoice_item_id',f"""
SELECT fii.fiscal_invoice_item_id,fii.fiscal_invoice_id,fi.invoice_no,ds.supplier_key,dm.material_key,
 CAST(date_format(CAST(fi.received_at AS DATE),'yyyyMMdd') AS INT) received_date_key,
 fii.quantity,fii.unit_price,fii.merchandise_value,fii.icms_amount,fii.ipi_amount,fii.pis_amount,fii.cofins_amount
FROM {fq(silver_schema,'fiscal_invoice_item')} fii
JOIN {fq(silver_schema,'fiscal_invoice')} fi ON fi.fiscal_invoice_id=fii.fiscal_invoice_id
JOIN {fq(gold_schema,'dim_supplier')} ds ON ds.supplier_id=fi.supplier_id AND ds.is_current=TRUE
JOIN {fq(gold_schema,'dim_material')} dm ON dm.material_id=fii.material_id AND dm.is_current=TRUE
""", ['supplier_key','material_key'])

merge('fact_accounts_payable','accounts_payable_id',f"""
SELECT ap.accounts_payable_id,ap.fiscal_invoice_id,ds.supplier_key,
 CAST(date_format(CAST(ap.posted_at AS DATE),'yyyyMMdd') AS INT) posted_date_key,
 CAST(date_format(CAST(ap.due_at AS DATE),'yyyyMMdd') AS INT) due_date_key,
 CASE WHEN ap.paid_at IS NOT NULL THEN CAST(date_format(CAST(ap.paid_at AS DATE),'yyyyMMdd') AS INT) END paid_date_key,
 ap.status,ap.amount,ap.posted_at,ap.due_at,ap.scheduled_at,ap.payment_scheduled_for,ap.paid_at,ap.voided_at,ap.days_to_pay,ap.is_overdue,ap.days_past_due
FROM {fq(silver_schema,'accounts_payable')} ap
JOIN {fq(gold_schema,'dim_supplier')} ds ON ds.supplier_id=ap.supplier_id AND ds.is_current=TRUE
""", ['supplier_key'])

merge('fact_payment','payment_id',f"""
SELECT p.payment_id,p.payment_no,p.accounts_payable_id,ap.supplier_key,
 CAST(date_format(CAST(p.paid_at AS DATE),'yyyyMMdd') AS INT) payment_date_key,p.amount,p.paid_at
FROM {fq(silver_schema,'payment')} p
JOIN {fq(gold_schema,'fact_accounts_payable')} ap ON ap.accounts_payable_id=p.accounts_payable_id
""", ['supplier_key'])
