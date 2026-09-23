# Databricks notebook source
# COMMAND ----------
def _widget(name,default):
    try:return dbutils.widgets.get(name)
    except Exception: dbutils.widgets.text(name,default); return dbutils.widgets.get(name)
catalog=_widget('catalog','mro-data'); bronze_schema=_widget('bronze_schema','bronze'); silver_schema=_widget('silver_schema','silver')
def q(x): return f'`{x}`'
def fq(s,o): return f'{q(catalog)}.{q(s)}.{q(o)}'
def schema_fq(s): return f'{q(catalog)}.{q(s)}'
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_fq(silver_schema)}")
spark.sql(f"""CREATE OR REPLACE TABLE {fq(silver_schema,'purchase_requisition')} USING DELTA AS
WITH ranked AS (SELECT *,ROW_NUMBER() OVER(PARTITION BY id ORDER BY _source_commit_version DESC,_source_commit_timestamp DESC,_source_record_hash DESC) rn FROM {fq(bronze_schema,'purchase_requisition_state_raw')}),
terminal AS (SELECT entity_id AS purchase_requisition_id,MAX(CASE WHEN to_state='REJECTED' THEN occurred_at END) rejected_at,MAX(CASE WHEN to_state='CANCELLED' THEN occurred_at END) cancelled_at FROM {fq(bronze_schema,'entity_state_transition_raw')} WHERE entity_type='purchase_requisition' GROUP BY entity_id)
SELECT r.id AS purchase_requisition_id,r.request_no,r.warehouse_id,r.source_type,r.source_ref,r.priority,r.status,r.requested_at,r.approve_after,r.approved_at,t.rejected_at,t.cancelled_at,
 CASE WHEN r.approved_at IS NOT NULL THEN timestampdiff(SECOND,r.requested_at,r.approved_at)/3600.0 END AS approval_lead_hours,
 r._source_commit_timestamp AS source_commit_at,r._bronze_ingested_at AS bronze_ingested_at
FROM ranked r LEFT JOIN terminal t ON t.purchase_requisition_id=r.id WHERE r.rn=1""")
spark.sql(f"""CREATE OR REPLACE TABLE {fq(silver_schema,'purchase_requisition_item')} USING DELTA AS
SELECT i.id AS purchase_requisition_item_id,i.requisition_id AS purchase_requisition_id,i.material_id,m.description AS material_description,m.category AS material_category,i.quantity,
 i._source_commit_timestamp AS source_commit_at,i._bronze_ingested_at AS bronze_ingested_at
FROM {fq(bronze_schema,'purchase_requisition_item_raw')} i JOIN {fq(silver_schema,'material')} m ON m.material_id=i.material_id""")
spark.sql(f"""CREATE OR REPLACE TABLE {fq(silver_schema,'purchase_order')} USING DELTA AS
WITH ranked AS (SELECT *,ROW_NUMBER() OVER(PARTITION BY id ORDER BY _source_commit_version DESC,_source_commit_timestamp DESC,_source_record_hash DESC) rn FROM {fq(bronze_schema,'purchase_order_state_raw')}),
cancelled AS (SELECT entity_id AS purchase_order_id,MAX(occurred_at) cancelled_at FROM {fq(bronze_schema,'entity_state_transition_raw')} WHERE entity_type='purchase_order' AND to_state='CANCELLED' GROUP BY entity_id)
SELECT po.id AS purchase_order_id,po.po_no,po.requisition_id AS purchase_requisition_id,po.supplier_id,s.supplier_name,po.warehouse_id,w.warehouse_name,po.status,po.approved_at,po.send_after,po.sent_at,po.promised_at,po.next_receipt_at,po.received_at,po.closed_at,c.cancelled_at,po.receipt_count,
 CASE WHEN po.sent_at IS NOT NULL THEN timestampdiff(SECOND,po.approved_at,po.sent_at)/3600.0 END AS dispatch_lead_hours,
 CASE WHEN po.received_at IS NOT NULL THEN timestampdiff(SECOND,po.promised_at,po.received_at)/3600.0 END AS delivery_variance_hours,
 CASE WHEN po.received_at IS NULL THEN NULL WHEN po.received_at<=po.promised_at THEN TRUE ELSE FALSE END AS received_on_time,
 po._source_commit_timestamp AS source_commit_at,po._bronze_ingested_at AS bronze_ingested_at
FROM ranked po JOIN {fq(silver_schema,'supplier')} s ON s.supplier_id=po.supplier_id JOIN {fq(silver_schema,'warehouse')} w ON w.warehouse_id=po.warehouse_id LEFT JOIN cancelled c ON c.purchase_order_id=po.id WHERE po.rn=1""")
spark.sql(f"""CREATE OR REPLACE TABLE {fq(silver_schema,'purchase_order_item')} USING DELTA AS
WITH ranked AS (SELECT *,ROW_NUMBER() OVER(PARTITION BY id ORDER BY _source_commit_version DESC,_source_commit_timestamp DESC,_source_record_hash DESC) rn FROM {fq(bronze_schema,'purchase_order_item_state_raw')})
SELECT poi.id AS purchase_order_item_id,poi.purchase_order_id,poi.requisition_item_id AS purchase_requisition_item_id,poi.material_id,m.description AS material_description,m.category AS material_category,poi.quantity AS ordered_qty,poi.received_qty,GREATEST(poi.quantity-poi.received_qty,0.0) AS open_qty,
 CAST(poi.unit_price_cents/100.0 AS DECIMAL(18,2)) AS unit_price,CAST(poi.quantity*poi.unit_price_cents/100.0 AS DECIMAL(18,2)) AS ordered_value,CAST(poi.received_qty*poi.unit_price_cents/100.0 AS DECIMAL(18,2)) AS received_value,
 CASE WHEN poi.quantity=0 THEN 1.0 ELSE LEAST(poi.received_qty/poi.quantity,1.0) END AS fill_ratio,poi._source_commit_timestamp AS source_commit_at,poi._bronze_ingested_at AS bronze_ingested_at
FROM ranked poi JOIN {fq(silver_schema,'material')} m ON m.material_id=poi.material_id WHERE poi.rn=1""")
spark.sql(f"""CREATE OR REPLACE TABLE {fq(silver_schema,'goods_receipt')} USING DELTA AS
SELECT id AS goods_receipt_id,receipt_no,purchase_order_id,warehouse_id,received_at,_source_commit_timestamp AS source_commit_at,_bronze_ingested_at AS bronze_ingested_at FROM {fq(bronze_schema,'goods_receipt_raw')}""")
spark.sql(f"""CREATE OR REPLACE TABLE {fq(silver_schema,'goods_receipt_item')} USING DELTA AS
SELECT gri.id AS goods_receipt_item_id,gri.goods_receipt_id,gri.purchase_order_item_id,gri.material_id,m.description AS material_description,gri.qty_received,gri._source_commit_timestamp AS source_commit_at,gri._bronze_ingested_at AS bronze_ingested_at
FROM {fq(bronze_schema,'goods_receipt_item_raw')} gri JOIN {fq(silver_schema,'material')} m ON m.material_id=gri.material_id""")
violations=spark.sql(f"SELECT COUNT(*) n FROM {fq(silver_schema,'purchase_order_item')} WHERE received_qty>ordered_qty+1e-9 OR ordered_qty<0 OR received_qty<0").first()['n']
if violations: raise RuntimeError(f'Invalid PO quantities detected: {violations} rows')
print('Silver procurement built from per-table CDC state')
