# Databricks notebook source
# Incremental procurement facts with SCD2 keys fixed at first insert.
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
        spark.sql(f"MERGE INTO {fq(gold_schema,target)} t USING {q(view)} s ON t.{q(pk)}=s.{q(pk)} WHEN MATCHED THEN UPDATE SET {upd} WHEN NOT MATCHED THEN INSERT *")

merge('fact_purchase_order_item','purchase_order_item_id',f"""
SELECT poi.purchase_order_item_id,poi.purchase_order_id,po.po_no,ds.supplier_key,dm.material_key,dw.warehouse_key,
 CAST(date_format(CAST(po.approved_at AS DATE),'yyyyMMdd') AS INT) order_date_key,
 CASE WHEN po.received_at IS NOT NULL THEN CAST(date_format(CAST(po.received_at AS DATE),'yyyyMMdd') AS INT) END received_date_key,
 po.status,poi.ordered_qty,poi.received_qty,poi.open_qty,poi.unit_price,poi.ordered_value,poi.received_value,poi.fill_ratio,
 po.promised_at,po.received_at,po.cancelled_at,po.delivery_variance_hours,po.received_on_time,
 (poi.received_qty>0 AND poi.received_qty<poi.ordered_qty) is_partial,
 (poi.received_qty>=poi.ordered_qty AND po.received_on_time=TRUE) is_on_time_in_full
FROM {fq(silver_schema,'purchase_order_item')} poi
JOIN {fq(silver_schema,'purchase_order')} po ON po.purchase_order_id=poi.purchase_order_id
JOIN {fq(gold_schema,'dim_supplier')} ds ON ds.supplier_id=po.supplier_id AND ds.is_current=TRUE
JOIN {fq(gold_schema,'dim_material')} dm ON dm.material_id=poi.material_id AND dm.is_current=TRUE
JOIN {fq(gold_schema,'dim_warehouse')} dw ON dw.warehouse_id=po.warehouse_id
""", ['supplier_key','material_key','warehouse_key'])

merge('fact_goods_receipt_item','goods_receipt_item_id',f"""
SELECT gri.goods_receipt_item_id,gri.goods_receipt_id,gri.purchase_order_item_id,po.purchase_order_id,
 ds.supplier_key,dm.material_key,dw.warehouse_key,
 CAST(date_format(CAST(gr.received_at AS DATE),'yyyyMMdd') AS INT) receipt_date_key,
 gri.qty_received,poi.unit_price,CAST(gri.qty_received*poi.unit_price AS DECIMAL(18,2)) receipt_value,gr.received_at
FROM {fq(silver_schema,'goods_receipt_item')} gri
JOIN {fq(silver_schema,'goods_receipt')} gr ON gr.goods_receipt_id=gri.goods_receipt_id
JOIN {fq(silver_schema,'purchase_order_item')} poi ON poi.purchase_order_item_id=gri.purchase_order_item_id
JOIN {fq(silver_schema,'purchase_order')} po ON po.purchase_order_id=gr.purchase_order_id
JOIN {fq(gold_schema,'dim_supplier')} ds ON ds.supplier_id=po.supplier_id AND ds.is_current=TRUE
JOIN {fq(gold_schema,'dim_material')} dm ON dm.material_id=gri.material_id AND dm.is_current=TRUE
JOIN {fq(gold_schema,'dim_warehouse')} dw ON dw.warehouse_id=gr.warehouse_id
""", ['supplier_key','material_key','warehouse_key'])
