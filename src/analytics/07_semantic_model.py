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
# Unity Catalog semantic layer using metric views.
#
# Project invariant:
#   every Gold fact table owns one primary analytical cube.
#
# A "cube" in this project is implemented as a Unity Catalog metric view:
# one fact source + many-to-one dimension joins + governed measures.
#
# This keeps grains explicit and prevents accidental fan-out between facts.

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_fq(semantic_schema)}")
spark.sql(f"USE CATALOG {q(catalog)}")

# COMMAND ----------
CUBE_FACT_MAP = {
    # Maintenance
    "maintenance_metrics": "fact_work_order",
    "maintenance_material_metrics": "fact_work_order_material",

    # Inventory
    "inventory_metrics": "fact_inventory_movement",
    "inventory_position_metrics": "fact_stock_position",

    # Procurement / receiving
    "procurement_metrics": "fact_purchase_order_item",
    "receiving_metrics": "fact_goods_receipt_item",

    # Fiscal / finance
    "invoice_metrics": "fact_fiscal_invoice",
    "fiscal_item_metrics": "fact_fiscal_invoice_item",
    "payables_metrics": "fact_accounts_payable",
    "payment_metrics": "fact_payment",
}

if len(CUBE_FACT_MAP) != len(set(CUBE_FACT_MAP.values())):
    raise RuntimeError("Each primary semantic cube must map to a distinct Gold fact table.")

# COMMAND ----------
maintenance_yaml = f"""
version: 1.1
comment: "Work-order lifecycle and maintenance performance cube"
source: {gold_schema}.fact_work_order
joins:
  - name: asset
    source: {gold_schema}.dim_asset
    'on': source.asset_key = asset.asset_key
    rely:
      at_most_one_match: true
  - name: cost_center
    source: {gold_schema}.dim_cost_center
    'on': source.cost_center_key = cost_center.cost_center_key
    rely:
      at_most_one_match: true
  - name: warehouse
    source: {gold_schema}.dim_warehouse
    'on': source.warehouse_key = warehouse.warehouse_key
    rely:
      at_most_one_match: true
  - name: planned_date
    source: {gold_schema}.dim_date
    'on': source.planned_date_key = planned_date.date_key
    rely:
      at_most_one_match: true
fields:
  - name: planned_date
    expr: planned_date.full_date
  - name: planned_year
    expr: planned_date.year
  - name: planned_month
    expr: planned_date.month
  - name: maintenance_type
    expr: source.maintenance_type
  - name: priority
    expr: source.priority
  - name: status
    expr: source.status
  - name: asset_type
    expr: asset.asset_type
  - name: asset_criticality
    expr: asset.criticality
  - name: cost_center
    expr: cost_center.cost_center_name
  - name: warehouse
    expr: warehouse.warehouse_name
measures:
  - name: work_order_count
    expr: COUNT(1)
  - name: completed_work_order_count
    expr: SUM(CASE WHEN source.status IN ('COMPLETED', 'CLOSED') THEN 1 ELSE 0 END)
  - name: closed_work_order_count
    expr: SUM(CASE WHEN source.status = 'CLOSED' THEN 1 ELSE 0 END)
  - name: corrective_work_order_count
    expr: SUM(CASE WHEN source.maintenance_type = 'CORRECTIVE' THEN 1 ELSE 0 END)
  - name: cancelled_work_order_count
    expr: SUM(CASE WHEN source.status = 'CANCELLED' THEN 1 ELSE 0 END)
  - name: waiting_material_work_order_count
    expr: SUM(CASE WHEN source.waited_for_material = 1 THEN 1 ELSE 0 END)
  - name: estimated_material_cost
    expr: SUM(source.estimated_material_cost)
  - name: avg_planning_hours
    expr: AVG(source.planning_hours)
  - name: avg_material_wait_hours
    expr: AVG(source.material_wait_hours)
  - name: avg_execution_hours
    expr: AVG(source.execution_hours)
  - name: avg_cycle_hours
    expr: AVG(source.elapsed_cycle_hours)
  - name: material_wait_ratio
    expr: SUM(source.material_wait_hours) / NULLIF(SUM(source.elapsed_cycle_hours), 0)
"""

maintenance_material_yaml = f"""
version: 1.1
comment: "Maintenance material demand, fulfillment and consumption cube"
source: {gold_schema}.fact_work_order_material
joins:
  - name: material
    source: {gold_schema}.dim_material
    'on': source.material_key = material.material_key
    rely:
      at_most_one_match: true
  - name: asset
    source: {gold_schema}.dim_asset
    'on': source.asset_key = asset.asset_key
    rely:
      at_most_one_match: true
  - name: cost_center
    source: {gold_schema}.dim_cost_center
    'on': source.cost_center_key = cost_center.cost_center_key
    rely:
      at_most_one_match: true
  - name: warehouse
    source: {gold_schema}.dim_warehouse
    'on': source.warehouse_key = warehouse.warehouse_key
    rely:
      at_most_one_match: true
  - name: planned_date
    source: {gold_schema}.dim_date
    'on': source.planned_date_key = planned_date.date_key
    rely:
      at_most_one_match: true
fields:
  - name: planned_date
    expr: planned_date.full_date
  - name: material
    expr: material.description
  - name: material_category
    expr: material.category
  - name: material_criticality
    expr: material.criticality
  - name: asset_type
    expr: asset.asset_type
  - name: cost_center
    expr: cost_center.cost_center_name
  - name: warehouse
    expr: warehouse.warehouse_name
  - name: maintenance_type
    expr: source.maintenance_type
  - name: priority
    expr: source.priority
  - name: work_order_status
    expr: source.work_order_status
  - name: material_status
    expr: source.status
measures:
  - name: material_requirement_line_count
    expr: COUNT(1)
  - name: work_order_count
    expr: COUNT(DISTINCT source.work_order_id)
  - name: required_quantity
    expr: SUM(source.qty_required)
  - name: issued_quantity
    expr: SUM(source.qty_issued)
  - name: open_quantity
    expr: SUM(source.open_qty)
  - name: quantity_fulfillment_rate
    expr: SUM(source.qty_issued) / NULLIF(SUM(source.qty_required), 0)
  - name: fully_fulfilled_line_count
    expr: SUM(CASE WHEN source.status = 'FULFILLED' THEN 1 ELSE 0 END)
  - name: partially_issued_line_count
    expr: SUM(CASE WHEN source.status = 'PARTIALLY_ISSUED' THEN 1 ELSE 0 END)
  - name: cancelled_material_line_count
    expr: SUM(CASE WHEN source.status = 'CANCELLED' THEN 1 ELSE 0 END)
  - name: estimated_issued_material_cost
    expr: SUM(source.estimated_issued_cost)
"""

inventory_yaml = f"""
version: 1.1
comment: "Inventory movement and material-flow cube"
source: {gold_schema}.fact_inventory_movement
joins:
  - name: material
    source: {gold_schema}.dim_material
    'on': source.material_key = material.material_key
    rely:
      at_most_one_match: true
  - name: warehouse
    source: {gold_schema}.dim_warehouse
    'on': source.warehouse_key = warehouse.warehouse_key
    rely:
      at_most_one_match: true
  - name: movement_date
    source: {gold_schema}.dim_date
    'on': source.movement_date_key = movement_date.date_key
    rely:
      at_most_one_match: true
fields:
  - name: movement_date
    expr: movement_date.full_date
  - name: movement_type
    expr: source.movement_type
  - name: movement_direction
    expr: source.movement_direction
  - name: reference_type
    expr: source.reference_type
  - name: material
    expr: material.description
  - name: material_category
    expr: material.category
  - name: material_criticality
    expr: material.criticality
  - name: warehouse
    expr: warehouse.warehouse_name
measures:
  - name: movement_count
    expr: COUNT(1)
  - name: issued_quantity
    expr: SUM(CASE WHEN source.movement_type = 'ISSUE' THEN -source.quantity ELSE 0 END)
  - name: received_quantity
    expr: SUM(CASE WHEN source.movement_type = 'RECEIPT' THEN source.quantity ELSE 0 END)
  - name: net_quantity
    expr: SUM(source.quantity)
  - name: absolute_movement_quantity
    expr: SUM(ABS(source.quantity))
  - name: net_movement_value
    expr: SUM(source.movement_value)
  - name: absolute_movement_value
    expr: SUM(ABS(source.movement_value))
  - name: adjustment_quantity
    expr: SUM(CASE WHEN source.movement_type = 'CYCLE_COUNT_ADJUSTMENT' THEN source.quantity ELSE 0 END)
"""

inventory_position_yaml = f"""
version: 1.1
comment: "Current stock position, availability, reorder and inventory-value cube"
source: {gold_schema}.fact_stock_position
joins:
  - name: material
    source: {gold_schema}.dim_material
    'on': source.material_key = material.material_key
    rely:
      at_most_one_match: true
  - name: warehouse
    source: {gold_schema}.dim_warehouse
    'on': source.warehouse_key = warehouse.warehouse_key
    rely:
      at_most_one_match: true
  - name: snapshot_date
    source: {gold_schema}.dim_date
    'on': source.snapshot_date_key = snapshot_date.date_key
    rely:
      at_most_one_match: true
fields:
  - name: snapshot_date
    expr: snapshot_date.full_date
  - name: material
    expr: material.description
  - name: material_category
    expr: material.category
  - name: material_criticality
    expr: material.criticality
  - name: warehouse
    expr: warehouse.warehouse_name
measures:
  - name: stock_position_count
    expr: COUNT(1)
  - name: on_hand_quantity
    expr: SUM(source.on_hand)
  - name: reserved_quantity
    expr: SUM(source.reserved)
  - name: available_quantity
    expr: SUM(source.available)
  - name: inventory_value
    expr: SUM(source.inventory_value)
  - name: below_reorder_position_count
    expr: SUM(CASE WHEN source.available <= source.reorder_point THEN 1 ELSE 0 END)
  - name: out_of_stock_position_count
    expr: SUM(CASE WHEN source.available <= 0 THEN 1 ELSE 0 END)
  - name: average_available_quantity
    expr: AVG(source.available)
"""

procurement_yaml = f"""
version: 1.1
comment: "Purchase-order line and supplier-performance cube"
source: {gold_schema}.fact_purchase_order_item
joins:
  - name: supplier
    source: {gold_schema}.dim_supplier
    'on': source.supplier_key = supplier.supplier_key
    rely:
      at_most_one_match: true
  - name: material
    source: {gold_schema}.dim_material
    'on': source.material_key = material.material_key
    rely:
      at_most_one_match: true
  - name: warehouse
    source: {gold_schema}.dim_warehouse
    'on': source.warehouse_key = warehouse.warehouse_key
    rely:
      at_most_one_match: true
  - name: order_date
    source: {gold_schema}.dim_date
    'on': source.order_date_key = order_date.date_key
    rely:
      at_most_one_match: true
fields:
  - name: order_date
    expr: order_date.full_date
  - name: supplier
    expr: supplier.supplier_name
  - name: material
    expr: material.description
  - name: material_category
    expr: material.category
  - name: warehouse
    expr: warehouse.warehouse_name
  - name: po_status
    expr: source.status
  - name: promised_date
    expr: CAST(source.promised_at AS DATE)
measures:
  - name: purchase_order_count
    expr: COUNT(DISTINCT source.purchase_order_id)
  - name: purchase_order_item_count
    expr: COUNT(1)
  - name: ordered_value
    expr: SUM(source.ordered_value)
  - name: received_value
    expr: SUM(source.received_value)
  - name: open_value
    expr: SUM(source.ordered_value - source.received_value)
  - name: ordered_quantity
    expr: SUM(source.ordered_qty)
  - name: received_quantity
    expr: SUM(source.received_qty)
  - name: open_quantity
    expr: SUM(source.open_qty)
  - name: fill_rate
    expr: SUM(source.received_qty) / NULLIF(SUM(source.ordered_qty), 0)
  - name: avg_delivery_variance_hours
    expr: AVG(source.delivery_variance_hours)
  - name: late_purchase_order_count
    expr: COUNT(DISTINCT CASE WHEN source.received_on_time = FALSE THEN source.purchase_order_id END)
  - name: cancelled_purchase_order_count
    expr: COUNT(DISTINCT CASE WHEN source.status = 'CANCELLED' THEN source.purchase_order_id END)
  - name: otif_purchase_order_count
    expr: COUNT(DISTINCT CASE WHEN source.is_on_time_in_full = TRUE THEN source.purchase_order_id END)
  - name: partial_line_count
    expr: SUM(CASE WHEN source.is_partial THEN 1 ELSE 0 END)
"""

receiving_yaml = f"""
version: 1.1
comment: "Goods-receipt quantity and receipt-value cube"
source: {gold_schema}.fact_goods_receipt_item
joins:
  - name: supplier
    source: {gold_schema}.dim_supplier
    'on': source.supplier_key = supplier.supplier_key
    rely:
      at_most_one_match: true
  - name: material
    source: {gold_schema}.dim_material
    'on': source.material_key = material.material_key
    rely:
      at_most_one_match: true
  - name: warehouse
    source: {gold_schema}.dim_warehouse
    'on': source.warehouse_key = warehouse.warehouse_key
    rely:
      at_most_one_match: true
  - name: receipt_date
    source: {gold_schema}.dim_date
    'on': source.receipt_date_key = receipt_date.date_key
    rely:
      at_most_one_match: true
fields:
  - name: receipt_date
    expr: receipt_date.full_date
  - name: supplier
    expr: supplier.supplier_name
  - name: material
    expr: material.description
  - name: material_category
    expr: material.category
  - name: warehouse
    expr: warehouse.warehouse_name
measures:
  - name: goods_receipt_count
    expr: COUNT(DISTINCT source.goods_receipt_id)
  - name: receipt_line_count
    expr: COUNT(1)
  - name: purchase_order_count
    expr: COUNT(DISTINCT source.purchase_order_id)
  - name: received_quantity
    expr: SUM(source.qty_received)
  - name: receipt_value
    expr: SUM(source.receipt_value)
  - name: avg_unit_price
    expr: AVG(source.unit_price)
  - name: avg_receipt_value
    expr: AVG(source.receipt_value)
"""

invoice_yaml = f"""
version: 1.1
comment: "Fiscal-invoice lifecycle, exception and value cube"
source: {gold_schema}.fact_fiscal_invoice
joins:
  - name: supplier
    source: {gold_schema}.dim_supplier
    'on': source.supplier_key = supplier.supplier_key
    rely:
      at_most_one_match: true
  - name: received_date
    source: {gold_schema}.dim_date
    'on': source.received_date_key = received_date.date_key
    rely:
      at_most_one_match: true
fields:
  - name: received_date
    expr: received_date.full_date
  - name: supplier
    expr: supplier.supplier_name
  - name: invoice_status
    expr: source.status
  - name: tax_issue
    expr: source.tax_issue
  - name: was_blocked
    expr: source.was_blocked
measures:
  - name: invoice_count
    expr: COUNT(1)
  - name: invoice_value
    expr: SUM(source.total_amount)
  - name: subtotal_value
    expr: SUM(source.subtotal)
  - name: tax_value
    expr: SUM(source.tax_total)
  - name: posted_invoice_count
    expr: SUM(CASE WHEN source.status = 'POSTED' THEN 1 ELSE 0 END)
  - name: released_invoice_count
    expr: SUM(CASE WHEN source.status = 'RELEASED' THEN 1 ELSE 0 END)
  - name: blocked_invoice_count
    expr: SUM(CASE WHEN source.was_blocked THEN 1 ELSE 0 END)
  - name: blocked_invoice_rate
    expr: SUM(CASE WHEN source.was_blocked THEN 1 ELSE 0 END) / NULLIF(COUNT(1), 0)
  - name: avg_abs_price_variance_pct
    expr: AVG(ABS(source.price_variance_pct))
  - name: tax_issue_count
    expr: SUM(CASE WHEN source.tax_issue THEN 1 ELSE 0 END)
"""

fiscal_item_yaml = f"""
version: 1.1
comment: "Fiscal invoice-item merchandise and tax-composition cube"
source: {gold_schema}.fact_fiscal_invoice_item
joins:
  - name: supplier
    source: {gold_schema}.dim_supplier
    'on': source.supplier_key = supplier.supplier_key
    rely:
      at_most_one_match: true
  - name: material
    source: {gold_schema}.dim_material
    'on': source.material_key = material.material_key
    rely:
      at_most_one_match: true
  - name: received_date
    source: {gold_schema}.dim_date
    'on': source.received_date_key = received_date.date_key
    rely:
      at_most_one_match: true
fields:
  - name: received_date
    expr: received_date.full_date
  - name: supplier
    expr: supplier.supplier_name
  - name: material
    expr: material.description
  - name: material_category
    expr: material.category
  - name: material_ncm
    expr: material.ncm
measures:
  - name: fiscal_item_count
    expr: COUNT(1)
  - name: invoice_count
    expr: COUNT(DISTINCT source.fiscal_invoice_id)
  - name: invoiced_quantity
    expr: SUM(source.quantity)
  - name: merchandise_value
    expr: SUM(source.merchandise_value)
  - name: icms_amount
    expr: SUM(source.icms_amount)
  - name: ipi_amount
    expr: SUM(source.ipi_amount)
  - name: pis_amount
    expr: SUM(source.pis_amount)
  - name: cofins_amount
    expr: SUM(source.cofins_amount)
  - name: total_tax_amount
    expr: SUM(source.icms_amount + source.ipi_amount + source.pis_amount + source.cofins_amount)
  - name: effective_tax_rate
    expr: SUM(source.icms_amount + source.ipi_amount + source.pis_amount + source.cofins_amount) / NULLIF(SUM(source.merchandise_value), 0)
"""

payables_yaml = f"""
version: 1.1
comment: "Accounts-payable exposure, aging and payment-performance cube"
source: {gold_schema}.fact_accounts_payable
joins:
  - name: supplier
    source: {gold_schema}.dim_supplier
    'on': source.supplier_key = supplier.supplier_key
    rely:
      at_most_one_match: true
  - name: posted_date
    source: {gold_schema}.dim_date
    'on': source.posted_date_key = posted_date.date_key
    rely:
      at_most_one_match: true
  - name: due_date
    source: {gold_schema}.dim_date
    'on': source.due_date_key = due_date.date_key
    rely:
      at_most_one_match: true
fields:
  - name: posted_date
    expr: posted_date.full_date
  - name: due_date
    expr: due_date.full_date
  - name: supplier
    expr: supplier.supplier_name
  - name: ap_status
    expr: source.status
  - name: is_overdue
    expr: source.is_overdue
measures:
  - name: payable_count
    expr: COUNT(1)
  - name: payable_amount
    expr: SUM(source.amount)
  - name: open_amount
    expr: SUM(CASE WHEN source.status NOT IN ('PAID', 'VOIDED') THEN source.amount ELSE 0 END)
  - name: scheduled_amount
    expr: SUM(CASE WHEN source.status = 'SCHEDULED' THEN source.amount ELSE 0 END)
  - name: paid_amount
    expr: SUM(CASE WHEN source.status = 'PAID' THEN source.amount ELSE 0 END)
  - name: voided_amount
    expr: SUM(CASE WHEN source.status = 'VOIDED' THEN source.amount ELSE 0 END)
  - name: voided_count
    expr: SUM(CASE WHEN source.status = 'VOIDED' THEN 1 ELSE 0 END)
  - name: overdue_amount
    expr: SUM(CASE WHEN source.is_overdue THEN source.amount ELSE 0 END)
  - name: overdue_count
    expr: SUM(CASE WHEN source.is_overdue THEN 1 ELSE 0 END)
  - name: avg_days_to_pay
    expr: AVG(source.days_to_pay)
  - name: avg_days_past_due
    expr: AVG(source.days_past_due)
"""

payment_yaml = f"""
version: 1.1
comment: "Executed-payment cash-outflow cube"
source: {gold_schema}.fact_payment
joins:
  - name: supplier
    source: {gold_schema}.dim_supplier
    'on': source.supplier_key = supplier.supplier_key
    rely:
      at_most_one_match: true
  - name: payment_date
    source: {gold_schema}.dim_date
    'on': source.payment_date_key = payment_date.date_key
    rely:
      at_most_one_match: true
fields:
  - name: payment_date
    expr: payment_date.full_date
  - name: payment_year
    expr: payment_date.year
  - name: payment_month
    expr: payment_date.month
  - name: supplier
    expr: supplier.supplier_name
measures:
  - name: payment_count
    expr: COUNT(1)
  - name: paid_payable_count
    expr: COUNT(DISTINCT source.accounts_payable_id)
  - name: payment_amount
    expr: SUM(source.amount)
  - name: avg_payment_amount
    expr: AVG(source.amount)
  - name: max_payment_amount
    expr: MAX(source.amount)
"""

views = {
    "maintenance_metrics": maintenance_yaml,
    "maintenance_material_metrics": maintenance_material_yaml,
    "inventory_metrics": inventory_yaml,
    "inventory_position_metrics": inventory_position_yaml,
    "procurement_metrics": procurement_yaml,
    "receiving_metrics": receiving_yaml,
    "invoice_metrics": invoice_yaml,
    "fiscal_item_metrics": fiscal_item_yaml,
    "payables_metrics": payables_yaml,
    "payment_metrics": payment_yaml,
}

if set(views) != set(CUBE_FACT_MAP):
    raise RuntimeError("Semantic cube definitions and CUBE_FACT_MAP are inconsistent.")

for name, definition in views.items():
    sql = f"""CREATE OR REPLACE VIEW {fq(semantic_schema, name)}
WITH METRICS
LANGUAGE YAML
AS $$
{definition.strip()}
$$
"""
    spark.sql(sql)
    print(
        f"Created analytical cube {fq(semantic_schema, name)} "
        f"from {fq(gold_schema, CUBE_FACT_MAP[name])}"
    )

# COMMAND ----------
# Human- and machine-readable semantic coverage registry.
#
# This is a normal view, not a metric view. It makes the design invariant
# auditable by the quality-gate task and easy to inspect in Catalog Explorer.

registry_rows = [
    (
        "maintenance_metrics",
        "fact_work_order",
        "maintenance",
        "one row per work order",
    ),
    (
        "maintenance_material_metrics",
        "fact_work_order_material",
        "maintenance",
        "one row per work-order material requirement",
    ),
    (
        "inventory_metrics",
        "fact_inventory_movement",
        "inventory",
        "one row per inventory movement",
    ),
    (
        "inventory_position_metrics",
        "fact_stock_position",
        "inventory",
        "one row per material x warehouse snapshot",
    ),
    (
        "procurement_metrics",
        "fact_purchase_order_item",
        "procurement",
        "one row per purchase-order item",
    ),
    (
        "receiving_metrics",
        "fact_goods_receipt_item",
        "procurement",
        "one row per goods-receipt item",
    ),
    (
        "invoice_metrics",
        "fact_fiscal_invoice",
        "fiscal",
        "one row per fiscal invoice",
    ),
    (
        "fiscal_item_metrics",
        "fact_fiscal_invoice_item",
        "fiscal",
        "one row per fiscal invoice item",
    ),
    (
        "payables_metrics",
        "fact_accounts_payable",
        "finance",
        "one row per accounts-payable title",
    ),
    (
        "payment_metrics",
        "fact_payment",
        "finance",
        "one row per executed payment",
    ),
]

values_sql = ",\n".join(
    "("
    + ", ".join(sql_string(value) for value in row)
    + ")"
    for row in registry_rows
)

spark.sql(f"""
CREATE OR REPLACE VIEW {fq(semantic_schema, "cube_registry")} AS
SELECT *
FROM VALUES
{values_sql}
AS t(cube_name, fact_table, domain, grain)
""")

print("Semantic coverage: 10 Gold facts -> 10 primary analytical cubes")
