# Databricks notebook source
# MRO analytical pipeline notebook.
# This file is intended to be imported as a Databricks notebook source file.

# COMMAND ----------

def _widget(name: str, default: str) -> str:
    try:
        return dbutils.widgets.get(name)
    except Exception:
        dbutils.widgets.text(name, default)
        return dbutils.widgets.get(name)

catalog = _widget("catalog", "mro-data")
source_schema = _widget("source_schema", "mro_sim")
bronze_schema = _widget("bronze_schema", "bronze")
silver_schema = _widget("silver_schema", "silver")
gold_schema = _widget("gold_schema", "gold")
semantic_schema = _widget("semantic_schema", "semantic")

def q(identifier: str) -> str:
    if "`" in identifier:
        raise ValueError(f"Backticks are not allowed in identifier: {identifier!r}")
    return f"`{identifier}`"

def fq(schema: str, object_name: str) -> str:
    return f"{q(catalog)}.{q(schema)}.{q(object_name)}"

def schema_fq(schema: str) -> str:
    return f"{q(catalog)}.{q(schema)}"

def sql_string(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


# COMMAND ----------
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_fq(gold_schema)}")

# COMMAND ----------
spark.sql(f"""
CREATE OR REPLACE TABLE {fq(gold_schema, "fact_purchase_order_item")}
USING DELTA AS
SELECT
    poi.purchase_order_item_id,
    poi.purchase_order_id,
    po.po_no,
    ds.supplier_key,
    dm.material_key,
    dw.warehouse_key,
    CAST(date_format(CAST(po.approved_at AS DATE), 'yyyyMMdd') AS INT) AS order_date_key,
    CASE WHEN po.received_at IS NOT NULL
         THEN CAST(date_format(CAST(po.received_at AS DATE), 'yyyyMMdd') AS INT) END
         AS received_date_key,
    po.status,
    poi.ordered_qty,
    poi.received_qty,
    poi.open_qty,
    poi.unit_price,
    poi.ordered_value,
    poi.received_value,
    poi.fill_ratio,
    po.promised_at,
    po.received_at,
    po.cancelled_at,
    po.delivery_variance_hours,
    po.received_on_time,
    (poi.received_qty > 0 AND poi.received_qty < poi.ordered_qty) AS is_partial,
    (poi.received_qty >= poi.ordered_qty AND po.received_on_time = TRUE) AS is_on_time_in_full
FROM {fq(silver_schema, "purchase_order_item")} poi
JOIN {fq(silver_schema, "purchase_order")} po
  ON po.purchase_order_id = poi.purchase_order_id
JOIN {fq(gold_schema, "dim_supplier")} ds
  ON ds.supplier_id = po.supplier_id
JOIN {fq(gold_schema, "dim_material")} dm
  ON dm.material_id = poi.material_id
JOIN {fq(gold_schema, "dim_warehouse")} dw
  ON dw.warehouse_id = po.warehouse_id
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {fq(gold_schema, "fact_goods_receipt_item")}
USING DELTA AS
SELECT
    gri.goods_receipt_item_id,
    gri.goods_receipt_id,
    gri.purchase_order_item_id,
    po.purchase_order_id,
    ds.supplier_key,
    dm.material_key,
    dw.warehouse_key,
    CAST(date_format(CAST(gr.received_at AS DATE), 'yyyyMMdd') AS INT) AS receipt_date_key,
    gri.qty_received,
    poi.unit_price,
    CAST(gri.qty_received * poi.unit_price AS DECIMAL(18,2)) AS receipt_value,
    gr.received_at
FROM {fq(silver_schema, "goods_receipt_item")} gri
JOIN {fq(silver_schema, "goods_receipt")} gr
  ON gr.goods_receipt_id = gri.goods_receipt_id
JOIN {fq(silver_schema, "purchase_order_item")} poi
  ON poi.purchase_order_item_id = gri.purchase_order_item_id
JOIN {fq(silver_schema, "purchase_order")} po
  ON po.purchase_order_id = gr.purchase_order_id
JOIN {fq(gold_schema, "dim_supplier")} ds
  ON ds.supplier_id = po.supplier_id
JOIN {fq(gold_schema, "dim_material")} dm
  ON dm.material_id = gri.material_id
JOIN {fq(gold_schema, "dim_warehouse")} dw
  ON dw.warehouse_id = gr.warehouse_id
""")

print("Gold procurement facts built")
