# Databricks notebook source
# Process-mining event log and derived case/transition models.

# COMMAND ----------
def _widget(n,d):
    try:return dbutils.widgets.get(n)
    except Exception: dbutils.widgets.text(n,d); return dbutils.widgets.get(n)
catalog=_widget('catalog','mro-data'); bronze_schema=_widget('bronze_schema','bronze'); gold_schema=_widget('gold_schema','gold')
def q(x):return f'`{x}`'
def fq(s,o):return f'{q(catalog)}.{q(s)}.{q(o)}'
def schema_fq(s):return f'{q(catalog)}.{q(s)}'
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_fq(gold_schema)}")

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW _src_process_event AS
WITH e AS (
 SELECT id transition_id,concat(entity_type,':',entity_id) case_id,entity_type,entity_id,
        from_state,to_state activity,reason,occurred_at,_source_commit_timestamp source_commit_at
 FROM {fq(bronze_schema,'entity_state_transition_raw')}
), x AS (
 SELECT *,lag(occurred_at) OVER(PARTITION BY case_id ORDER BY occurred_at,source_commit_at,transition_id) previous_event_at,
        row_number() OVER(PARTITION BY case_id ORDER BY occurred_at,source_commit_at,transition_id) event_index
 FROM e
)
SELECT *,CASE WHEN previous_event_at IS NULL THEN NULL ELSE timestampdiff(SECOND,previous_event_at,occurred_at) END seconds_since_previous
FROM x
""")
if not spark.catalog.tableExists(fq(gold_schema,'process_event')):
    spark.sql(f"CREATE TABLE {fq(gold_schema,'process_event')} USING DELTA AS SELECT * FROM _src_process_event")
else:
    spark.sql(f"MERGE INTO {fq(gold_schema,'process_event')} t USING _src_process_event s ON t.transition_id=s.transition_id WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")

spark.sql(f"""
CREATE OR REPLACE TABLE {fq(gold_schema,'process_case_summary')} USING DELTA AS
SELECT case_id,entity_type,entity_id,MIN(occurred_at) case_started_at,MAX(occurred_at) case_last_event_at,
       COUNT(*) event_count,MAX(event_index) transition_count,
       timestampdiff(SECOND,MIN(occurred_at),MAX(occurred_at)) case_duration_seconds,
       max_by(activity,struct(occurred_at,source_commit_at,transition_id)) current_state
FROM {fq(gold_schema,'process_event')} GROUP BY case_id,entity_type,entity_id
""")
spark.sql(f"""
CREATE OR REPLACE TABLE {fq(gold_schema,'process_transition_summary')} USING DELTA AS
SELECT entity_type,from_state,activity to_state,COUNT(*) transition_count,
       AVG(seconds_since_previous) avg_seconds_from_previous,
       percentile_approx(seconds_since_previous,0.5) median_seconds_from_previous
FROM {fq(gold_schema,'process_event')} GROUP BY entity_type,from_state,activity
""")
print('Process mining models published')
