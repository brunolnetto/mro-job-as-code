# Databricks notebook source
# COMMAND ----------
def _widget(n,d):
    try:return dbutils.widgets.get(n)
    except Exception: dbutils.widgets.text(n,d); return dbutils.widgets.get(n)
catalog=_widget('catalog','mro-data'); bronze_schema=_widget('bronze_schema','bronze'); silver_schema=_widget('silver_schema','silver'); gold_schema=_widget('gold_schema','gold')
def q(x):return f'`{x}`'
def fq(s,o):return f'{q(catalog)}.{q(s)}.{q(o)}'

def insert_only(target,pk,sql):
    view=f'_src_{target}'; spark.sql(f"CREATE OR REPLACE TEMP VIEW {q(view)} AS {sql}")
    if not spark.catalog.tableExists(fq(gold_schema,target)):
        spark.sql(f"CREATE TABLE {fq(gold_schema,target)} USING DELTA AS SELECT * FROM {q(view)}")
    else:
        spark.sql(f"MERGE INTO {fq(gold_schema,target)} t USING {q(view)} s ON t.{q(pk)}=s.{q(pk)} WHEN NOT MATCHED THEN INSERT *")

insert_only('fact_inventory_movement','inventory_movement_id',f"""
SELECT i.inventory_movement_id,dm.material_key,dw.warehouse_key,
 CAST(date_format(CAST(i.occurred_at AS DATE),'yyyyMMdd') AS INT) movement_date_key,
 i.movement_type,i.movement_direction,i.quantity,i.unit_cost,i.movement_value,i.reference_type,i.reference_id,i.occurred_at
FROM {fq(silver_schema,'inventory_movement')} i
JOIN {fq(gold_schema,'dim_material')} dm ON dm.material_id=i.material_id AND dm.is_current=TRUE
JOIN {fq(gold_schema,'dim_warehouse')} dw ON dw.warehouse_id=i.warehouse_id
""")

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW _src_stock_position AS
SELECT s.material_id,s.warehouse_id,dm.material_key,dw.warehouse_key,
 CAST(date_format(CAST(h.data_horizon_at AS DATE),'yyyyMMdd') AS INT) snapshot_date_key,
 h.data_horizon_at snapshot_at,s.on_hand,s.reserved,s.available,s.reorder_point,s.reorder_qty,s.inventory_value,s.last_counted_at
FROM {fq(silver_schema,'stock_position')} s
CROSS JOIN (SELECT data_horizon_at FROM {fq(bronze_schema,'ingestion_run')} WHERE completed_at IS NOT NULL ORDER BY completed_at DESC LIMIT 1) h
JOIN {fq(gold_schema,'dim_material')} dm ON dm.material_id=s.material_id AND dm.is_current=TRUE
JOIN {fq(gold_schema,'dim_warehouse')} dw ON dw.warehouse_id=s.warehouse_id
""")
if not spark.catalog.tableExists(fq(gold_schema,'fact_stock_position')):
    spark.sql(f"CREATE TABLE {fq(gold_schema,'fact_stock_position')} USING DELTA AS SELECT * FROM _src_stock_position")
else:
    spark.sql(f"""
    MERGE INTO {fq(gold_schema,'fact_stock_position')} t USING _src_stock_position s
    ON t.material_id=s.material_id AND t.warehouse_id=s.warehouse_id
    WHEN MATCHED THEN UPDATE SET
      t.material_key=s.material_key,t.warehouse_key=s.warehouse_key,
      t.snapshot_date_key=s.snapshot_date_key,t.snapshot_at=s.snapshot_at,t.on_hand=s.on_hand,t.reserved=s.reserved,
      t.available=s.available,t.reorder_point=s.reorder_point,t.reorder_qty=s.reorder_qty,t.inventory_value=s.inventory_value,t.last_counted_at=s.last_counted_at
    WHEN NOT MATCHED THEN INSERT *
    """)
