# Databricks notebook source
# Selected conformed dimensions use SCD Type 2 from Bronze master CDC history.

# COMMAND ----------
def _widget(name, default):
    try: return dbutils.widgets.get(name)
    except Exception: dbutils.widgets.text(name, default); return dbutils.widgets.get(name)

catalog=_widget('catalog','mro-data'); bronze_schema=_widget('bronze_schema','bronze'); silver_schema=_widget('silver_schema','silver'); gold_schema=_widget('gold_schema','gold')
def q(x): return f'`{x}`'
def fq(s,o): return f'{q(catalog)}.{q(s)}.{q(o)}'
def schema_fq(s): return f'{q(catalog)}.{q(s)}'
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_fq(gold_schema)}")

def scd2(target, natural_key, source_sql):
    base=f'_base_{target}_versions'
    src=f'_src_{target}_versions'
    spark.sql(f"CREATE OR REPLACE TEMP VIEW {q(base)} AS {source_sql}")
    spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW {q(src)} AS
    SELECT *, CAST(NULL AS TIMESTAMP) AS effective_to, TRUE AS is_current
    FROM {q(base)}
    """)
    if not spark.catalog.tableExists(fq(gold_schema,target)):
        spark.sql(f"CREATE TABLE {fq(gold_schema,target)} USING DELTA AS SELECT * FROM {q(src)}")
    else:
        spark.sql(f"MERGE INTO {fq(gold_schema,target)} t USING {q(src)} s ON t.dimension_key=s.dimension_key WHEN NOT MATCHED THEN INSERT *")
    win=f'_win_{target}'
    spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW {q(win)} AS
    SELECT dimension_key,
           LEAD(effective_from) OVER(PARTITION BY {q(natural_key)} ORDER BY effective_from, source_commit_version) next_from
    FROM {fq(gold_schema,target)}
    """)
    spark.sql(f"""
    MERGE INTO {fq(gold_schema,target)} t USING {q(win)} w
    ON t.dimension_key=w.dimension_key
    WHEN MATCHED THEN UPDATE SET t.effective_to=w.next_from, t.is_current=(w.next_from IS NULL)
    """)

scd2('dim_supplier','supplier_id',f"""
WITH x AS (
 SELECT id supplier_id,name supplier_name,tax_id,
        CAST(lead_time_days AS INT) lead_time_days,
        CAST(payment_term_days AS INT) payment_term_days,
        CAST(delivery_reliability AS DECIMAL(9,6)) delivery_reliability,
        CAST(price_variability AS DECIMAL(9,6)) price_variability,active,
        _source_commit_version source_commit_version,
        _source_commit_timestamp effective_from,
        sha2(concat_ws('||',name,coalesce(tax_id,''),lead_time_days,payment_term_days,delivery_reliability,price_variability,active),256) attr_hash
 FROM {fq(bronze_schema,'supplier_raw')}
), y AS (
 SELECT *,lag(attr_hash) OVER(PARTITION BY supplier_id ORDER BY source_commit_version) prev_hash FROM x
)
SELECT xxhash64(supplier_id,CAST(source_commit_version AS STRING)) dimension_key,
       xxhash64(supplier_id,CAST(source_commit_version AS STRING)) supplier_key,
       supplier_id,supplier_name,tax_id,lead_time_days,payment_term_days,
       delivery_reliability,price_variability,active,source_commit_version,effective_from,attr_hash
FROM y WHERE prev_hash IS NULL OR prev_hash<>attr_hash
""")

scd2('dim_material','material_id',f"""
WITH x AS (
 SELECT id material_id,description,category,uom,ncm,criticality,
        CAST(unit_cost_cents/100.0 AS DECIMAL(18,2)) unit_cost,
        reorder_point,reorder_qty,preferred_supplier_id,
        CAST(icms_bps/100.0 AS DECIMAL(9,4)) icms_pct,
        CAST(ipi_bps/100.0 AS DECIMAL(9,4)) ipi_pct,
        CAST(pis_bps/100.0 AS DECIMAL(9,4)) pis_pct,
        CAST(cofins_bps/100.0 AS DECIMAL(9,4)) cofins_pct,active,
        _source_commit_version source_commit_version,
        _source_commit_timestamp effective_from,
        sha2(concat_ws('||',description,category,uom,coalesce(ncm,''),criticality,unit_cost_cents,reorder_point,reorder_qty,preferred_supplier_id,icms_bps,ipi_bps,pis_bps,cofins_bps,active),256) attr_hash
 FROM {fq(bronze_schema,'material_raw')}
), y AS (
 SELECT *,lag(attr_hash) OVER(PARTITION BY material_id ORDER BY source_commit_version) prev_hash FROM x
)
SELECT xxhash64(material_id,CAST(source_commit_version AS STRING)) dimension_key,
       xxhash64(material_id,CAST(source_commit_version AS STRING)) material_key,
       material_id,description,category,uom,ncm,criticality,unit_cost,reorder_point,reorder_qty,
       preferred_supplier_id,icms_pct,ipi_pct,pis_pct,cofins_pct,active,
       source_commit_version,effective_from,attr_hash
FROM y WHERE prev_hash IS NULL OR prev_hash<>attr_hash
""")

scd2('dim_asset','asset_id',f"""
WITH x AS (
 SELECT id asset_id,description,asset_type,criticality,cost_center_id,warehouse_id,active,
        _source_commit_version source_commit_version,
        _source_commit_timestamp effective_from,
        sha2(concat_ws('||',description,asset_type,criticality,cost_center_id,warehouse_id,active),256) attr_hash
 FROM {fq(bronze_schema,'asset_raw')}
), y AS (
 SELECT *,lag(attr_hash) OVER(PARTITION BY asset_id ORDER BY source_commit_version) prev_hash FROM x
)
SELECT xxhash64(asset_id,CAST(source_commit_version AS STRING)) dimension_key,
       xxhash64(asset_id,CAST(source_commit_version AS STRING)) asset_key,
       asset_id,description,asset_type,criticality,cost_center_id,warehouse_id,active,
       source_commit_version,effective_from,attr_hash
FROM y WHERE prev_hash IS NULL OR prev_hash<>attr_hash
""")

def type1(target,key,sql):
    view=f'_src_{target}'
    spark.sql(f"CREATE OR REPLACE TEMP VIEW {q(view)} AS {sql}")
    if not spark.catalog.tableExists(fq(gold_schema,target)):
        spark.sql(f"CREATE TABLE {fq(gold_schema,target)} USING DELTA AS SELECT * FROM {q(view)}")
    else:
        spark.sql(f"MERGE INTO {fq(gold_schema,target)} t USING {q(view)} s ON t.{q(key)}=s.{q(key)} WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")

type1('dim_warehouse','warehouse_key',f"SELECT xxhash64(warehouse_id) warehouse_key,* FROM {fq(silver_schema,'warehouse')}")
type1('dim_cost_center','cost_center_key',f"SELECT xxhash64(cost_center_id) cost_center_key,* FROM {fq(silver_schema,'cost_center')}")

h=spark.sql(f"""
SELECT MIN(CAST(planned_at AS DATE)) mn,
       (SELECT CAST(data_horizon_at AS DATE) FROM {fq(bronze_schema,'ingestion_run')} WHERE completed_at IS NOT NULL ORDER BY completed_at DESC LIMIT 1) mx
FROM {fq(bronze_schema,'work_order_state_raw')}
""").first()
mn=h['mn'] or h['mx']; mx=h['mx']
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW _src_dates AS
WITH d AS (SELECT explode(sequence(DATE '{mn}', DATE '{mx}', INTERVAL 1 DAY)) full_date)
SELECT CAST(date_format(full_date,'yyyyMMdd') AS INT) date_key,full_date,year(full_date) year,
 quarter(full_date) quarter,month(full_date) month,date_format(full_date,'MMMM') month_name,
 weekofyear(full_date) week_of_year,day(full_date) day_of_month,dayofweek(full_date) day_of_week,
 dayofweek(full_date) IN (1,7) is_weekend FROM d
""")
if not spark.catalog.tableExists(fq(gold_schema,'dim_date')):
    spark.sql(f"CREATE TABLE {fq(gold_schema,'dim_date')} USING DELTA AS SELECT * FROM _src_dates")
else:
    spark.sql(f"MERGE INTO {fq(gold_schema,'dim_date')} t USING _src_dates s ON t.date_key=s.date_key WHEN NOT MATCHED THEN INSERT *")

print('Gold dimensions: supplier/material/asset=SCD2; warehouse/cost_center=Type1')
