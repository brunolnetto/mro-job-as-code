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
# Contract: normalize requisition, PO and receiving workflows; calculate
# procurement lifecycle attributes without aggregating away the source grain.

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_fq(silver_schema)}")

sim = spark.sql(
    f"SELECT simulated_at, committed_tick FROM {fq(bronze_schema, 'sim_state_raw')} WHERE id = 'main'"
).first()
committed_tick = int(sim["committed_tick"])

# COMMAND ----------
spark.sql(f"""
CREATE OR REPLACE TABLE {fq(silver_schema, "purchase_requisition")}
USING DELTA AS
WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY id ORDER BY valid_from_tick DESC
    ) rn
    FROM {fq(bronze_schema, "purchase_requisition_state_raw")}
    WHERE valid_from_tick <= {committed_tick}
)
SELECT
    id AS purchase_requisition_id,
    request_no,
    warehouse_id,
    source_type,
    source_ref,
    priority,
    status,
    requested_at,
    approve_after,
    approved_at,
    CASE WHEN approved_at IS NOT NULL
         THEN timestampdiff(SECOND, requested_at, approved_at) / 3600.0 END
         AS approval_lead_hours,
    valid_from_tick
FROM ranked
WHERE rn = 1
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {fq(silver_schema, "purchase_requisition_item")}
USING DELTA AS
SELECT
    i.id AS purchase_requisition_item_id,
    i.requisition_id AS purchase_requisition_id,
    i.material_id,
    m.description AS material_description,
    m.category AS material_category,
    i.quantity,
    i.simulation_tick
FROM {fq(bronze_schema, "purchase_requisition_item_raw")} i
JOIN {fq(silver_schema, "material")} m
  ON m.material_id = i.material_id
WHERE i.simulation_tick <= {committed_tick}
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {fq(silver_schema, "purchase_order")}
USING DELTA AS
WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY id ORDER BY valid_from_tick DESC
    ) rn
    FROM {fq(bronze_schema, "purchase_order_state_raw")}
    WHERE valid_from_tick <= {committed_tick}
)
SELECT
    po.id AS purchase_order_id,
    po.po_no,
    po.requisition_id AS purchase_requisition_id,
    po.supplier_id,
    s.supplier_name,
    po.warehouse_id,
    w.warehouse_name,
    po.status,
    po.approved_at,
    po.send_after,
    po.sent_at,
    po.promised_at,
    po.next_receipt_at,
    po.received_at,
    po.closed_at,
    po.receipt_count,
    CASE WHEN po.sent_at IS NOT NULL
         THEN timestampdiff(SECOND, po.approved_at, po.sent_at) / 3600.0 END
         AS dispatch_lead_hours,
    CASE WHEN po.received_at IS NOT NULL
         THEN timestampdiff(SECOND, po.promised_at, po.received_at) / 3600.0 END
         AS delivery_variance_hours,
    CASE
      WHEN po.received_at IS NULL THEN NULL
      WHEN po.received_at <= po.promised_at THEN TRUE
      ELSE FALSE
    END AS received_on_time,
    po.valid_from_tick
FROM ranked po
JOIN {fq(silver_schema, "supplier")} s
  ON s.supplier_id = po.supplier_id
JOIN {fq(silver_schema, "warehouse")} w
  ON w.warehouse_id = po.warehouse_id
WHERE po.rn = 1
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {fq(silver_schema, "purchase_order_item")}
USING DELTA AS
WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY id ORDER BY valid_from_tick DESC
    ) rn
    FROM {fq(bronze_schema, "purchase_order_item_state_raw")}
    WHERE valid_from_tick <= {committed_tick}
)
SELECT
    poi.id AS purchase_order_item_id,
    poi.purchase_order_id,
    poi.requisition_item_id AS purchase_requisition_item_id,
    poi.material_id,
    m.description AS material_description,
    m.category AS material_category,
    poi.quantity AS ordered_qty,
    poi.received_qty,
    GREATEST(poi.quantity - poi.received_qty, 0.0) AS open_qty,
    CAST(poi.unit_price_cents / 100.0 AS DECIMAL(18,2)) AS unit_price,
    CAST(poi.quantity * poi.unit_price_cents / 100.0 AS DECIMAL(18,2)) AS ordered_value,
    CAST(poi.received_qty * poi.unit_price_cents / 100.0 AS DECIMAL(18,2)) AS received_value,
    CASE WHEN poi.quantity = 0 THEN 1.0
         ELSE LEAST(poi.received_qty / poi.quantity, 1.0) END AS fill_ratio,
    poi.valid_from_tick
FROM ranked poi
JOIN {fq(silver_schema, "material")} m
  ON m.material_id = poi.material_id
WHERE poi.rn = 1
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {fq(silver_schema, "goods_receipt")}
USING DELTA AS
SELECT
    id AS goods_receipt_id,
    receipt_no,
    purchase_order_id,
    warehouse_id,
    received_at,
    simulation_tick
FROM {fq(bronze_schema, "goods_receipt_raw")}
WHERE simulation_tick <= {committed_tick}
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {fq(silver_schema, "goods_receipt_item")}
USING DELTA AS
SELECT
    gri.id AS goods_receipt_item_id,
    gri.goods_receipt_id,
    gri.purchase_order_item_id,
    gri.material_id,
    m.description AS material_description,
    gri.qty_received,
    gri.simulation_tick
FROM {fq(bronze_schema, "goods_receipt_item_raw")} gri
JOIN {fq(silver_schema, "material")} m
  ON m.material_id = gri.material_id
WHERE gri.simulation_tick <= {committed_tick}
""")

# COMMAND ----------
violations = spark.sql(
    f"""
    SELECT COUNT(*) n
    FROM {fq(silver_schema, "purchase_order_item")}
    WHERE received_qty > ordered_qty + 1e-9
       OR ordered_qty < 0
       OR received_qty < 0
    """
).first()["n"]

if violations:
    raise RuntimeError(f"Invalid PO quantities detected: {violations} rows")

print(f"Silver procurement built at committed_tick={committed_tick}")
