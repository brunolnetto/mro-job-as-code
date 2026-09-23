# Databricks notebook source
# COMMAND ----------
def _widget(name,default):
    try:return dbutils.widgets.get(name)
    except Exception: dbutils.widgets.text(name,default); return dbutils.widgets.get(name)
catalog=_widget('catalog','mro-data'); bronze_schema=_widget('bronze_schema','bronze'); silver_schema=_widget('silver_schema','silver')
def q(x): return f'`{x}`'
def fq(s,o): return f'{q(catalog)}.{q(s)}.{q(o)}'
def schema_fq(s): return f'{q(catalog)}.{q(s)}'
def sql_string(v): return "'"+str(v).replace("'","''")+"'"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_fq(silver_schema)}")
h=spark.sql(f"SELECT data_horizon_at FROM {fq(bronze_schema,'ingestion_run')} WHERE completed_at IS NOT NULL ORDER BY completed_at DESC LIMIT 1").first()
if h is None: raise RuntimeError('No completed Bronze ingestion_run')
data_horizon_at=h['data_horizon_at']
spark.sql(f"""CREATE OR REPLACE TABLE {fq(silver_schema,'fiscal_invoice')} USING DELTA AS
WITH ranked AS (SELECT *,ROW_NUMBER() OVER(PARTITION BY id ORDER BY _source_commit_version DESC,_source_commit_timestamp DESC,_source_record_hash DESC) rn FROM {fq(bronze_schema,'fiscal_invoice_state_raw')}),
blocked AS (SELECT entity_id AS invoice_id,1 AS was_blocked FROM {fq(bronze_schema,'entity_state_transition_raw')} WHERE entity_type='fiscal_invoice' AND to_state='BLOCKED' GROUP BY entity_id)
SELECT fi.id AS fiscal_invoice_id,fi.invoice_no,fi.supplier_id,s.supplier_name,fi.purchase_order_id,fi.goods_receipt_id,fi.status,fi.issued_at,fi.received_at,fi.validate_after,fi.validation_complete_after,fi.blocked_until,
 CAST(fi.subtotal_cents/100.0 AS DECIMAL(18,2)) AS subtotal,CAST(fi.tax_total_cents/100.0 AS DECIMAL(18,2)) AS tax_total,CAST(fi.total_amount_cents/100.0 AS DECIMAL(18,2)) AS total_amount,CAST(fi.price_variance_bps/100.0 AS DECIMAL(9,4)) AS price_variance_pct,fi.tax_issue,COALESCE(b.was_blocked,0)=1 AS was_blocked,
 fi._source_commit_timestamp AS source_commit_at,fi._bronze_ingested_at AS bronze_ingested_at
FROM ranked fi JOIN {fq(silver_schema,'supplier')} s ON s.supplier_id=fi.supplier_id LEFT JOIN blocked b ON b.invoice_id=fi.id WHERE fi.rn=1""")
spark.sql(f"""CREATE OR REPLACE TABLE {fq(silver_schema,'fiscal_invoice_item')} USING DELTA AS
SELECT i.id AS fiscal_invoice_item_id,i.invoice_id AS fiscal_invoice_id,i.material_id,m.description AS material_description,i.quantity,CAST(i.unit_price_cents/100.0 AS DECIMAL(18,2)) AS unit_price,CAST(i.quantity*i.unit_price_cents/100.0 AS DECIMAL(18,2)) AS merchandise_value,CAST(i.icms_cents/100.0 AS DECIMAL(18,2)) AS icms_amount,CAST(i.ipi_cents/100.0 AS DECIMAL(18,2)) AS ipi_amount,CAST(i.pis_cents/100.0 AS DECIMAL(18,2)) AS pis_amount,CAST(i.cofins_cents/100.0 AS DECIMAL(18,2)) AS cofins_amount,i._source_commit_timestamp AS source_commit_at,i._bronze_ingested_at AS bronze_ingested_at
FROM {fq(bronze_schema,'fiscal_invoice_item_raw')} i JOIN {fq(silver_schema,'material')} m ON m.material_id=i.material_id""")
spark.sql(f"""CREATE OR REPLACE TABLE {fq(silver_schema,'accounts_payable')} USING DELTA AS
WITH ranked AS (SELECT *,ROW_NUMBER() OVER(PARTITION BY id ORDER BY _source_commit_version DESC,_source_commit_timestamp DESC,_source_record_hash DESC) rn FROM {fq(bronze_schema,'accounts_payable_state_raw')}),
voided AS (SELECT entity_id AS accounts_payable_id,MAX(occurred_at) AS voided_at FROM {fq(bronze_schema,'entity_state_transition_raw')} WHERE entity_type='accounts_payable' AND to_state='VOIDED' GROUP BY entity_id)
SELECT ap.id AS accounts_payable_id,ap.invoice_id AS fiscal_invoice_id,ap.supplier_id,s.supplier_name,ap.status,CAST(ap.amount_cents/100.0 AS DECIMAL(18,2)) AS amount,ap.posted_at,ap.due_at,ap.scheduled_at,ap.payment_scheduled_for,ap.paid_at,v.voided_at,
 CASE WHEN ap.paid_at IS NOT NULL THEN timestampdiff(DAY,ap.posted_at,ap.paid_at) END AS days_to_pay,
 CASE WHEN ap.status NOT IN ('PAID','VOIDED') AND ap.due_at<TIMESTAMP {sql_string(str(data_horizon_at))} THEN TRUE ELSE FALSE END AS is_overdue,
 CASE WHEN ap.status NOT IN ('PAID','VOIDED') AND ap.due_at<TIMESTAMP {sql_string(str(data_horizon_at))} THEN datediff(CAST(TIMESTAMP {sql_string(str(data_horizon_at))} AS DATE),CAST(ap.due_at AS DATE)) ELSE 0 END AS days_past_due,
 ap._source_commit_timestamp AS source_commit_at,ap._bronze_ingested_at AS bronze_ingested_at
FROM ranked ap JOIN {fq(silver_schema,'supplier')} s ON s.supplier_id=ap.supplier_id LEFT JOIN voided v ON v.accounts_payable_id=ap.id WHERE ap.rn=1""")
spark.sql(f"""CREATE OR REPLACE TABLE {fq(silver_schema,'payment')} USING DELTA AS
SELECT p.id AS payment_id,p.payment_no,p.accounts_payable_id,CAST(p.amount_cents/100.0 AS DECIMAL(18,2)) AS amount,p.paid_at,p._source_commit_timestamp AS source_commit_at,p._bronze_ingested_at AS bronze_ingested_at FROM {fq(bronze_schema,'payment_raw')} p""")
print(f'Silver finance built at data_horizon_at={data_horizon_at}')
