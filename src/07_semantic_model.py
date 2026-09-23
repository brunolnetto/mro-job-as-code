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
# Metric views require Unity Catalog and a supported DBR/SQL environment.
# The job uses the Gold facts as the governed source of measures.

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_fq(semantic_schema)}")
spark.sql(f"USE CATALOG {q(catalog)}")

# COMMAND ----------
maintenance_yaml = f"""
version: 1.1
comment: "MRO maintenance operational KPIs"
source: gold.fact_work_order
joins:
  - name: asset
    source: gold.dim_asset
    'on': source.asset_key = asset.asset_key
    rely:
      at_most_one_match: true
  - name: cost_center
    source: gold.dim_cost_center
    'on': source.cost_center_key = cost_center.cost_center_key
    rely:
      at_most_one_match: true
fields:
  - name: planned_date
    expr: CAST(source.planned_at AS DATE)
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
measures:
  - name: work_order_count
    expr: COUNT(1)
  - name: completed_work_order_count
    expr: SUM(CASE WHEN source.status IN ('COMPLETED', 'CLOSED') THEN 1 ELSE 0 END)
  - name: corrective_work_order_count
    expr: SUM(CASE WHEN source.maintenance_type = 'CORRECTIVE' THEN 1 ELSE 0 END)
  - name: estimated_material_cost
    expr: SUM(source.estimated_material_cost)
  - name: avg_material_wait_hours
    expr: AVG(source.material_wait_hours)
  - name: avg_execution_hours
    expr: AVG(source.execution_hours)
  - name: avg_cycle_hours
    expr: AVG(source.elapsed_cycle_hours)
  - name: material_wait_ratio
    expr: SUM(source.material_wait_hours) / NULLIF(SUM(source.elapsed_cycle_hours), 0)
"""

inventory_yaml = f"""
version: 1.1
comment: "Inventory movement KPIs"
source: gold.fact_inventory_movement
joins:
  - name: material
    source: gold.dim_material
    'on': source.material_key = material.material_key
    rely:
      at_most_one_match: true
  - name: warehouse
    source: gold.dim_warehouse
    'on': source.warehouse_key = warehouse.warehouse_key
    rely:
      at_most_one_match: true
fields:
  - name: movement_date
    expr: CAST(source.occurred_at AS DATE)
  - name: movement_type
    expr: source.movement_type
  - name: material_category
    expr: material.category
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
  - name: net_movement_value
    expr: SUM(source.movement_value)
  - name: adjustment_quantity
    expr: SUM(CASE WHEN source.movement_type = 'CYCLE_COUNT_ADJUSTMENT' THEN source.quantity ELSE 0 END)
"""

procurement_yaml = f"""
version: 1.1
comment: "Purchase order and supplier performance KPIs"
source: gold.fact_purchase_order_item
joins:
  - name: supplier
    source: gold.dim_supplier
    'on': source.supplier_key = supplier.supplier_key
    rely:
      at_most_one_match: true
  - name: material
    source: gold.dim_material
    'on': source.material_key = material.material_key
    rely:
      at_most_one_match: true
  - name: warehouse
    source: gold.dim_warehouse
    'on': source.warehouse_key = warehouse.warehouse_key
    rely:
      at_most_one_match: true
fields:
  - name: supplier
    expr: supplier.supplier_name
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
  - name: ordered_quantity
    expr: SUM(source.ordered_qty)
  - name: received_quantity
    expr: SUM(source.received_qty)
  - name: fill_rate
    expr: SUM(source.received_qty) / NULLIF(SUM(source.ordered_qty), 0)
  - name: avg_delivery_variance_hours
    expr: AVG(source.delivery_variance_hours)
  - name: late_purchase_order_count
    expr: COUNT(DISTINCT CASE WHEN source.received_on_time = FALSE THEN source.purchase_order_id END)
  - name: otif_purchase_order_count
    expr: COUNT(DISTINCT CASE WHEN source.is_on_time_in_full = TRUE THEN source.purchase_order_id END)
  - name: partial_line_count
    expr: SUM(CASE WHEN source.is_partial THEN 1 ELSE 0 END)
"""

invoice_yaml = f"""
version: 1.1
comment: "Fiscal invoice quality and matching KPIs"
source: gold.fact_fiscal_invoice
joins:
  - name: supplier
    source: gold.dim_supplier
    'on': source.supplier_key = supplier.supplier_key
    rely:
      at_most_one_match: true
fields:
  - name: supplier
    expr: supplier.supplier_name
  - name: invoice_status
    expr: source.status
  - name: received_date
    expr: CAST(source.received_at AS DATE)
measures:
  - name: invoice_count
    expr: COUNT(1)
  - name: invoice_value
    expr: SUM(source.total_amount)
  - name: blocked_invoice_count
    expr: SUM(CASE WHEN source.was_blocked THEN 1 ELSE 0 END)
  - name: blocked_invoice_rate
    expr: SUM(CASE WHEN source.was_blocked THEN 1 ELSE 0 END) / NULLIF(COUNT(1), 0)
  - name: avg_abs_price_variance_pct
    expr: AVG(ABS(source.price_variance_pct))
  - name: tax_issue_count
    expr: SUM(CASE WHEN source.tax_issue THEN 1 ELSE 0 END)
"""

payables_yaml = f"""
version: 1.1
comment: "Accounts payable KPIs"
source: gold.fact_accounts_payable
joins:
  - name: supplier
    source: gold.dim_supplier
    'on': source.supplier_key = supplier.supplier_key
    rely:
      at_most_one_match: true
fields:
  - name: supplier
    expr: supplier.supplier_name
  - name: ap_status
    expr: source.status
  - name: posted_date
    expr: CAST(source.posted_at AS DATE)
  - name: due_date
    expr: CAST(source.due_at AS DATE)
measures:
  - name: payable_count
    expr: COUNT(1)
  - name: payable_amount
    expr: SUM(source.amount)
  - name: open_amount
    expr: SUM(CASE WHEN source.status <> 'PAID' THEN source.amount ELSE 0 END)
  - name: paid_amount
    expr: SUM(CASE WHEN source.status = 'PAID' THEN source.amount ELSE 0 END)
  - name: overdue_amount
    expr: SUM(CASE WHEN source.is_overdue THEN source.amount ELSE 0 END)
  - name: overdue_count
    expr: SUM(CASE WHEN source.is_overdue THEN 1 ELSE 0 END)
  - name: avg_days_to_pay
    expr: AVG(source.days_to_pay)
"""

views = {
    "maintenance_metrics": maintenance_yaml,
    "inventory_metrics": inventory_yaml,
    "procurement_metrics": procurement_yaml,
    "invoice_metrics": invoice_yaml,
    "payables_metrics": payables_yaml,
}

for name, definition in views.items():
    sql = f"""CREATE OR REPLACE VIEW {fq(semantic_schema, name)}
WITH METRICS
LANGUAGE YAML
AS $$
{definition.strip()}
$$
"""
    spark.sql(sql)
    print(f"Created metric view: {fq(semantic_schema, name)}")

