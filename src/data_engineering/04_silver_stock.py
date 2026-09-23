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
spark.sql(f"""CREATE OR REPLACE TABLE {fq(silver_schema,'inventory_movement')} USING DELTA AS
SELECT i.id AS inventory_movement_id, i.material_id, m.description AS material_description,
 m.category AS material_category, i.warehouse_id, w.warehouse_name, i.movement_type, i.quantity,
 CASE WHEN i.quantity>0 THEN 'IN' WHEN i.quantity<0 THEN 'OUT' ELSE 'ZERO' END AS movement_direction,
 CAST(i.unit_cost_cents/100.0 AS DECIMAL(18,2)) AS unit_cost,
 CAST(i.quantity*i.unit_cost_cents/100.0 AS DECIMAL(18,2)) AS movement_value,
 i.reference_type, i.reference_id, i.occurred_at,
 i._source_commit_timestamp AS source_commit_at, i._bronze_ingested_at AS bronze_ingested_at
FROM {fq(bronze_schema,'inventory_movement_raw')} i
JOIN {fq(silver_schema,'material')} m ON m.material_id=i.material_id
JOIN {fq(silver_schema,'warehouse')} w ON w.warehouse_id=i.warehouse_id""")
spark.sql(f"""CREATE OR REPLACE TABLE {fq(silver_schema,'stock_position')} USING DELTA AS
WITH ranked AS (
 SELECT *, ROW_NUMBER() OVER (PARTITION BY material_id,warehouse_id
 ORDER BY _source_commit_version DESC,_source_commit_timestamp DESC,_source_record_hash DESC) rn
 FROM {fq(bronze_schema,'stock_balance_state_raw')})
SELECT s.material_id,m.description AS material_description,m.category AS material_category,
 s.warehouse_id,w.warehouse_name,s.on_hand,s.reserved,s.on_hand-s.reserved AS available,
 m.reorder_point,m.reorder_qty,CAST(s.on_hand*m.unit_cost AS DECIMAL(18,2)) AS inventory_value,
 s.last_counted_at,s._source_commit_timestamp AS source_commit_at,s._bronze_ingested_at AS bronze_ingested_at
FROM ranked s JOIN {fq(silver_schema,'material')} m ON m.material_id=s.material_id
JOIN {fq(silver_schema,'warehouse')} w ON w.warehouse_id=s.warehouse_id WHERE s.rn=1""")
violations=spark.sql(f"SELECT COUNT(*) n FROM {fq(silver_schema,'stock_position')} WHERE on_hand<0 OR reserved<0").first()['n']
if violations: raise RuntimeError(f'Negative inventory state detected: {violations} rows')
print('Silver stock built from per-table CDC state')
