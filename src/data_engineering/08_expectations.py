# Databricks notebook source
# Declarative expectations for the Jobs+MERGE architecture.

# COMMAND ----------
import json

def _widget(n,d):
    try:return dbutils.widgets.get(n)
    except Exception: dbutils.widgets.text(n,d); return dbutils.widgets.get(n)
catalog=_widget('catalog','mro-data'); silver_schema=_widget('silver_schema','silver'); gold_schema=_widget('gold_schema','gold'); ops_schema=_widget('ops_schema','ops')
def q(x):return f'`{x}`'
def fq(s,o):return f'{q(catalog)}.{q(s)}.{q(o)}'
def schema_fq(s):return f'{q(catalog)}.{q(s)}'

EXPECTATIONS = [
    {'dataset':'silver.stock_position','name':'non_negative_stock','expr':'on_hand >= 0 AND reserved >= 0','action':'fail'},
    {'dataset':'silver.work_order_material','name':'issued_not_above_required','expr':'qty_issued <= qty_required + 1e-9','action':'fail'},
    {'dataset':'silver.purchase_order_item','name':'received_not_above_ordered','expr':'received_qty <= ordered_qty + 1e-9','action':'fail'},
    {'dataset':'silver.accounts_payable','name':'paid_has_timestamp','expr':"status <> 'PAID' OR paid_at IS NOT NULL",'action':'fail'},
    {'dataset':'gold.dim_supplier','name':'valid_scd2_window','expr':'effective_to IS NULL OR effective_to > effective_from','action':'fail'},
    {'dataset':'gold.dim_material','name':'valid_scd2_window','expr':'effective_to IS NULL OR effective_to > effective_from','action':'fail'},
    {'dataset':'gold.dim_asset','name':'valid_scd2_window','expr':'effective_to IS NULL OR effective_to > effective_from','action':'fail'},
]

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_fq(ops_schema)}")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {fq(ops_schema,'expectation_result')} (
 observed_at TIMESTAMP,dataset STRING,expectation STRING,action STRING,
 total_rows BIGINT,failed_rows BIGINT,pass_rate DOUBLE
) USING DELTA
""")
failures=[]
for rule in EXPECTATIONS:
    layer,name=rule['dataset'].split('.',1)
    schema={'silver':silver_schema,'gold':gold_schema}[layer]
    row=spark.sql(f"SELECT COUNT(*) total,SUM(CASE WHEN NOT({rule['expr']}) THEN 1 ELSE 0 END) failed FROM {fq(schema,name)}").first()
    total=int(row['total'] or 0); failed=int(row['failed'] or 0)
    rate=1.0 if total==0 else (total-failed)/total
    spark.sql(f"INSERT INTO {fq(ops_schema,'expectation_result')} VALUES(current_timestamp(),'{rule['dataset']}','{rule['name']}','{rule['action']}',{total},{failed},{rate})")
    if failed and rule['action']=='fail': failures.append((rule['dataset'],rule['name'],failed))
print(json.dumps({'expectations':len(EXPECTATIONS),'failures':failures}))
if failures: raise RuntimeError(f'Declarative expectations failed: {failures}')
