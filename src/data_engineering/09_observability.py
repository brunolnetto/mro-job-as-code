# Databricks notebook source
# Row-count and Delta commit freshness observability.

# COMMAND ----------
from uuid import uuid4

def _widget(n,d):
    try:return dbutils.widgets.get(n)
    except Exception: dbutils.widgets.text(n,d); return dbutils.widgets.get(n)
catalog=_widget('catalog','mro-data'); bronze_schema=_widget('bronze_schema','bronze'); silver_schema=_widget('silver_schema','silver'); gold_schema=_widget('gold_schema','gold'); semantic_schema=_widget('semantic_schema','semantic'); ops_schema=_widget('ops_schema','ops')
def q(x):return f'`{x}`'
def fq(s,o):return f'{q(catalog)}.{q(s)}.{q(o)}'
def schema_fq(s):return f'{q(catalog)}.{q(s)}'

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_fq(ops_schema)}")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {fq(ops_schema,'asset_observation')} (
 run_id STRING,observed_at TIMESTAMP,layer STRING,asset_name STRING,asset_type STRING,
 row_count BIGINT,latest_delta_commit_at TIMESTAMP,freshness_minutes DOUBLE
) USING DELTA
""")
run_id=str(uuid4())
for layer,schema in [('bronze',bronze_schema),('silver',silver_schema),('gold',gold_schema)]:
    for row in spark.sql(f'SHOW TABLES IN {schema_fq(schema)}').collect():
        name=row['tableName']
        try:
            count=int(spark.sql(f'SELECT COUNT(*) FROM {fq(schema,name)}').first()[0])
            latest=spark.sql(f'DESCRIBE HISTORY {fq(schema,name)}').selectExpr('MAX(timestamp) latest').first()['latest']
            latest_sql='NULL' if latest is None else f"TIMESTAMP '{latest}'"
            spark.sql(f"""
            INSERT INTO {fq(ops_schema,'asset_observation')}
            VALUES('{run_id}',current_timestamp(),'{layer}','{name}','table',{count},{latest_sql},
                   CASE WHEN {latest_sql} IS NULL THEN NULL ELSE timestampdiff(SECOND,{latest_sql},current_timestamp())/60.0 END)
            """)
        except Exception as exc:
            print(f'observability skip {schema}.{name}: {exc}')
if spark.catalog.tableExists(fq(semantic_schema,'cube_registry')):
    count=int(spark.sql(f'SELECT COUNT(*) FROM {fq(semantic_schema,"cube_registry")}').first()[0])
    spark.sql(f"INSERT INTO {fq(ops_schema,'asset_observation')} VALUES('{run_id}',current_timestamp(),'semantic','cube_registry','view',{count},NULL,NULL)")
print({'observability_run_id':run_id})
