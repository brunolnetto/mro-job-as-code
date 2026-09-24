# Databricks notebook source
# Incremental stock/event models.

# COMMAND ----------
def _widget(name, default):
    try: return dbutils.widgets.get(name)
    except Exception: dbutils.widgets.text(name, default); return dbutils.widgets.get(name)

catalog=_widget('catalog','mro-data'); bronze_schema=_widget('bronze_schema','bronze'); silver_schema=_widget('silver_schema','silver')
def q(x): return f'`{x}`'
def fq(s,o): return f'{q(catalog)}.{q(s)}.{q(o)}'
def schema_fq(s): return f'{q(catalog)}.{q(s)}'
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_fq(silver_schema)}")

def merge(target, sql, keys):
    view=f'_src_{target}'
    spark.sql(f"CREATE OR REPLACE TEMP VIEW {q(view)} AS {sql}")
    if not spark.catalog.tableExists(fq(silver_schema,target)):
        spark.sql(f"CREATE TABLE {fq(silver_schema,target)} USING DELTA AS SELECT * FROM {q(view)}")
    else:
        on=' AND '.join(f't.{q(k)}=s.{q(k)}' for k in keys)
        spark.sql(f"MERGE INTO {fq(silver_schema,target)} t USING {q(view)} s ON {on} WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")

merge('inventory_movement', f"""
SELECT i.id inventory_movement_id, i.material_id, m.description material_description,
 m.category material_category, i.warehouse_id, w.warehouse_name, i.movement_type, i.quantity,
 CASE WHEN i.quantity>0 THEN 'IN' WHEN i.quantity<0 THEN 'OUT' ELSE 'ZERO' END movement_direction,
 CAST(i.unit_cost_cents/100.0 AS DECIMAL(18,2)) unit_cost,
 CAST(i.quantity*i.unit_cost_cents/100.0 AS DECIMAL(18,2)) movement_value,
 i.reference_type, i.reference_id, i.occurred_at,
 i._source_commit_timestamp source_commit_at, i._bronze_ingested_at bronze_ingested_at
FROM {fq(bronze_schema,'inventory_movement_raw')} i
JOIN {fq(silver_schema,'material')} m ON m.material_id=i.material_id
JOIN {fq(silver_schema,'warehouse')} w ON w.warehouse_id=i.warehouse_id
""", ['inventory_movement_id'])

merge('stock_position', f"""
WITH r AS (
 SELECT *, ROW_NUMBER() OVER(PARTITION BY material_id,warehouse_id ORDER BY _source_commit_version DESC,_source_commit_timestamp DESC,_source_record_hash DESC) rn
 FROM {fq(bronze_schema,'stock_balance_state_raw')}
)
SELECT s.material_id,m.description material_description,m.category material_category,
 s.warehouse_id,w.warehouse_name,s.on_hand,s.reserved,s.on_hand-s.reserved available,
 m.reorder_point,m.reorder_qty,CAST(s.on_hand*m.unit_cost AS DECIMAL(18,2)) inventory_value,
 s.last_counted_at,s._source_commit_timestamp source_commit_at,s._bronze_ingested_at bronze_ingested_at
FROM r s
JOIN {fq(silver_schema,'material')} m ON m.material_id=s.material_id
JOIN {fq(silver_schema,'warehouse')} w ON w.warehouse_id=s.warehouse_id
WHERE s.rn=1
""", ['material_id','warehouse_id'])

violations=int(spark.sql(f"SELECT COUNT(*) FROM {fq(silver_schema,'stock_position')} WHERE on_hand<0 OR reserved<0").first()[0])
if violations: raise RuntimeError(f'Negative inventory state: {violations}')
