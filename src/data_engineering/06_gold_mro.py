# Databricks notebook source
# Incremental Gold MRO facts. Dimension foreign keys are fixed on first insert,
# so later SCD2 changes do not rewrite historical attribution.

# COMMAND ----------
def _widget(n,d):
    try:return dbutils.widgets.get(n)
    except Exception: dbutils.widgets.text(n,d); return dbutils.widgets.get(n)
catalog=_widget('catalog','mro-data'); silver_schema=_widget('silver_schema','silver'); gold_schema=_widget('gold_schema','gold')
def q(x):return f'`{x}`'
def fq(s,o):return f'{q(catalog)}.{q(s)}.{q(o)}'
def schema_fq(s):return f'{q(catalog)}.{q(s)}'
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_fq(gold_schema)}")

def merge_preserve_dims(target,pk,sql,dim_cols):
    view=f'_src_{target}'
    spark.sql(f"CREATE OR REPLACE TEMP VIEW {q(view)} AS {sql}")
    if not spark.catalog.tableExists(fq(gold_schema,target)):
        spark.sql(f"CREATE TABLE {fq(gold_schema,target)} USING DELTA AS SELECT * FROM {q(view)}")
    else:
        cols=[r['col_name'] for r in spark.sql(f'DESCRIBE {fq(gold_schema,target)}').collect() if r['col_name'] and not r['col_name'].startswith('#')]
        update=', '.join(f't.{q(c)}=s.{q(c)}' for c in cols if c not in set(dim_cols+[pk]))
        spark.sql(f"MERGE INTO {fq(gold_schema,target)} t USING {q(view)} s ON t.{q(pk)}=s.{q(pk)} WHEN MATCHED THEN UPDATE SET {update} WHEN NOT MATCHED THEN INSERT *")

merge_preserve_dims('fact_work_order','work_order_id',f"""
SELECT wo.work_order_id,wo.order_no,da.asset_key,dcc.cost_center_key,dw.warehouse_key,
 CAST(date_format(CAST(wo.planned_at AS DATE),'yyyyMMdd') AS INT) planned_date_key,
 CASE WHEN wo.completed_at IS NOT NULL THEN CAST(date_format(CAST(wo.completed_at AS DATE),'yyyyMMdd') AS INT) END completed_date_key,
 CASE WHEN wo.closed_at IS NOT NULL THEN CAST(date_format(CAST(wo.closed_at AS DATE),'yyyyMMdd') AS INT) END closed_date_key,
 CASE WHEN wo.cancelled_at IS NOT NULL THEN CAST(date_format(CAST(wo.cancelled_at AS DATE),'yyyyMMdd') AS INT) END cancelled_date_key,
 wo.maintenance_type,wo.priority,wo.status,wo.planned_at,wo.released_at,wo.started_at,wo.completed_at,wo.closed_at,wo.cancelled_at,
 wo.planning_hours,wo.material_wait_hours,wo.execution_hours,wo.closure_hours,wo.elapsed_cycle_hours,
 wo.material_line_count,wo.fulfilled_material_line_count,wo.estimated_material_cost,wo.waited_for_material
FROM {fq(silver_schema,'work_order_lifecycle')} wo
JOIN {fq(silver_schema,'work_order')} wod ON wod.work_order_id=wo.work_order_id
JOIN {fq(gold_schema,'dim_asset')} da ON da.asset_id=wo.asset_id AND da.is_current=TRUE
JOIN {fq(gold_schema,'dim_cost_center')} dcc ON dcc.cost_center_id=wod.cost_center_id
JOIN {fq(gold_schema,'dim_warehouse')} dw ON dw.warehouse_id=wod.warehouse_id
""", ['asset_key','cost_center_key','warehouse_key'])

merge_preserve_dims('fact_work_order_material','work_order_material_id',f"""
SELECT wom.work_order_material_id,wom.work_order_id,dm.material_key,da.asset_key,dcc.cost_center_key,dw.warehouse_key,
 CAST(date_format(CAST(wo.planned_at AS DATE),'yyyyMMdd') AS INT) planned_date_key,
 wo.maintenance_type,wo.priority,wo.status work_order_status,wom.qty_required,wom.qty_issued,wom.open_qty,
 wom.fulfillment_ratio,wom.estimated_issued_cost,wom.status
FROM {fq(silver_schema,'work_order_material')} wom
JOIN {fq(silver_schema,'work_order')} wo ON wo.work_order_id=wom.work_order_id
JOIN {fq(gold_schema,'dim_material')} dm ON dm.material_id=wom.material_id AND dm.is_current=TRUE
JOIN {fq(gold_schema,'dim_asset')} da ON da.asset_id=wo.asset_id AND da.is_current=TRUE
JOIN {fq(gold_schema,'dim_cost_center')} dcc ON dcc.cost_center_id=wo.cost_center_id
JOIN {fq(gold_schema,'dim_warehouse')} dw ON dw.warehouse_id=wo.warehouse_id
""", ['material_key','asset_key','cost_center_key','warehouse_key'])
