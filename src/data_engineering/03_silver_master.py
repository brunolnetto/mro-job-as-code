# Databricks notebook source
# Incremental conformed master data. Bronze preserves master CDC history;
# Silver exposes one current row per durable business key.

# COMMAND ----------
def _widget(name, default):
    try:
        return dbutils.widgets.get(name)
    except Exception:
        dbutils.widgets.text(name, default)
        return dbutils.widgets.get(name)

catalog = _widget("catalog", "mro-data")
bronze_schema = _widget("bronze_schema", "bronze")
silver_schema = _widget("silver_schema", "silver")

def q(x): return f"`{x}`"
def fq(s, o): return f"{q(catalog)}.{q(s)}.{q(o)}"
def schema_fq(s): return f"{q(catalog)}.{q(s)}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_fq(silver_schema)}")


def upsert(target, source_sql, keys):
    view = f"_src_{target}"
    spark.sql(f"CREATE OR REPLACE TEMP VIEW {q(view)} AS {source_sql}")
    if not spark.catalog.tableExists(fq(silver_schema, target)):
        spark.sql(f"CREATE TABLE {fq(silver_schema, target)} USING DELTA AS SELECT * FROM {q(view)}")
    else:
        cond = " AND ".join(f"t.{q(k)} = s.{q(k)}" for k in keys)
        spark.sql(f"""
        MERGE INTO {fq(silver_schema, target)} t
        USING {q(view)} s
        ON {cond}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """)

# COMMAND ----------
upsert("supplier", f"""
WITH r AS (
 SELECT *, ROW_NUMBER() OVER(PARTITION BY id ORDER BY _source_commit_version DESC, _source_commit_timestamp DESC, _source_record_hash DESC) rn
 FROM {fq(bronze_schema, 'supplier_raw')}
)
SELECT id supplier_id, name supplier_name, tax_id,
 CAST(lead_time_days AS INT) lead_time_days,
 CAST(payment_term_days AS INT) payment_term_days,
 CAST(delivery_reliability AS DECIMAL(9,6)) delivery_reliability,
 CAST(price_variability AS DECIMAL(9,6)) price_variability,
 active, _source_commit_version source_commit_version,
 _source_commit_timestamp source_commit_at, _bronze_ingested_at source_ingested_at
FROM r WHERE rn = 1
""", ["supplier_id"])

upsert("warehouse", f"""
WITH r AS (
 SELECT *, ROW_NUMBER() OVER(PARTITION BY id ORDER BY _source_commit_version DESC, _source_commit_timestamp DESC, _source_record_hash DESC) rn
 FROM {fq(bronze_schema, 'warehouse_raw')}
)
SELECT id warehouse_id, name warehouse_name, active,
 _source_commit_version source_commit_version, _source_commit_timestamp source_commit_at,
 _bronze_ingested_at source_ingested_at
FROM r WHERE rn = 1
""", ["warehouse_id"])

upsert("cost_center", f"""
WITH r AS (
 SELECT *, ROW_NUMBER() OVER(PARTITION BY id ORDER BY _source_commit_version DESC, _source_commit_timestamp DESC, _source_record_hash DESC) rn
 FROM {fq(bronze_schema, 'cost_center_raw')}
)
SELECT id cost_center_id, name cost_center_name, active,
 _source_commit_version source_commit_version, _source_commit_timestamp source_commit_at,
 _bronze_ingested_at source_ingested_at
FROM r WHERE rn = 1
""", ["cost_center_id"])

upsert("material", f"""
WITH r AS (
 SELECT *, ROW_NUMBER() OVER(PARTITION BY id ORDER BY _source_commit_version DESC, _source_commit_timestamp DESC, _source_record_hash DESC) rn
 FROM {fq(bronze_schema, 'material_raw')}
)
SELECT id material_id, description, category, uom, ncm, criticality,
 CAST(unit_cost_cents / 100.0 AS DECIMAL(18,2)) unit_cost,
 reorder_point, reorder_qty, preferred_supplier_id,
 CAST(icms_bps / 100.0 AS DECIMAL(9,4)) icms_pct,
 CAST(ipi_bps / 100.0 AS DECIMAL(9,4)) ipi_pct,
 CAST(pis_bps / 100.0 AS DECIMAL(9,4)) pis_pct,
 CAST(cofins_bps / 100.0 AS DECIMAL(9,4)) cofins_pct,
 active, _source_commit_version source_commit_version,
 _source_commit_timestamp source_commit_at, _bronze_ingested_at source_ingested_at
FROM r WHERE rn = 1
""", ["material_id"])

upsert("asset", f"""
WITH r AS (
 SELECT *, ROW_NUMBER() OVER(PARTITION BY id ORDER BY _source_commit_version DESC, _source_commit_timestamp DESC, _source_record_hash DESC) rn
 FROM {fq(bronze_schema, 'asset_raw')}
)
SELECT a.id asset_id, a.description, a.asset_type, a.criticality,
 a.cost_center_id, cc.cost_center_name, a.warehouse_id, w.warehouse_name, a.active,
 a._source_commit_version source_commit_version, a._source_commit_timestamp source_commit_at,
 a._bronze_ingested_at source_ingested_at
FROM r a
LEFT JOIN {fq(silver_schema, 'cost_center')} cc ON cc.cost_center_id = a.cost_center_id
LEFT JOIN {fq(silver_schema, 'warehouse')} w ON w.warehouse_id = a.warehouse_id
WHERE a.rn = 1
""", ["asset_id"])

upsert("asset_material_profile", f"""
WITH r AS (
 SELECT *, ROW_NUMBER() OVER(PARTITION BY asset_id, material_id ORDER BY _source_commit_version DESC, _source_commit_timestamp DESC, _source_record_hash DESC) rn
 FROM {fq(bronze_schema, 'asset_material_profile_raw')}
)
SELECT p.asset_id, a.asset_type, p.material_id, m.category material_category,
 CAST(p.probability AS DECIMAL(9,6)) probability, p.min_qty, p.max_qty,
 p._source_commit_version source_commit_version, p._source_commit_timestamp source_commit_at,
 p._bronze_ingested_at source_ingested_at
FROM r p
JOIN {fq(silver_schema, 'asset')} a ON a.asset_id = p.asset_id
JOIN {fq(silver_schema, 'material')} m ON m.material_id = p.material_id
WHERE p.rn = 1
""", ["asset_id", "material_id"])

checks = {
    "duplicate_supplier": f"SELECT COUNT(*) FROM (SELECT supplier_id FROM {fq(silver_schema,'supplier')} GROUP BY supplier_id HAVING COUNT(*) > 1)",
    "duplicate_material": f"SELECT COUNT(*) FROM (SELECT material_id FROM {fq(silver_schema,'material')} GROUP BY material_id HAVING COUNT(*) > 1)",
    "material_without_supplier": f"SELECT COUNT(*) FROM {fq(silver_schema,'material')} m LEFT JOIN {fq(silver_schema,'supplier')} s ON s.supplier_id=m.preferred_supplier_id WHERE s.supplier_id IS NULL",
    "asset_without_cost_center": f"SELECT COUNT(*) FROM {fq(silver_schema,'asset')} WHERE cost_center_name IS NULL",
}
failures = {name: int(spark.sql(sql).first()[0]) for name, sql in checks.items()}
if any(failures.values()):
    raise RuntimeError(f"Silver master quality failure: {failures}")
print({"silver_master_merge": failures})
