# Databricks notebook source
# MAGIC %md
# MAGIC # Incremental MRO simulator
# MAGIC Notebook-task entry point. Parameters are read from Databricks widgets.

# COMMAND ----------

#!/usr/bin/env python3
"""Incremental MRO simulator for Databricks / Delta Lake.

The simulator is intentionally job-oriented:

* no local SQLite file;
* all durable state lives in Delta tables in a Unity Catalog schema;
* every invocation advances a persisted logical simulation clock;
* mutable entities are versioned by ``valid_from_tick``;
* user-facing views only expose versions/events at or before ``committed_tick``;
* deterministic IDs and per-tick RNG make retries reproducible;
* ``sim_state`` is advanced only after a micro-batch has been persisted.

The code is designed for a Databricks Notebook task. Runtime parameters are
read from Databricks widgets, so Jupyter/IPython kernel arguments never enter
the simulator. It requires only the PySpark/Delta libraries already available
in Databricks Runtime.
"""

from __future__ import annotations

import hashlib
import json
import math
import random
import traceback
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable
from zoneinfo import ZoneInfo

from pyspark.sql import Row, SparkSession
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# ---------------------------------------------------------------------------
# Static domain data
# ---------------------------------------------------------------------------

SUPPLIERS = [
    {
        "id": "SUP001",
        "name": "Rolamentos Brasil",
        "tax_id": "00000000000101",
        "lead_time_days": 3,
        "payment_term_days": 28,
        "delivery_reliability": 0.94,
        "price_variability": 0.025,
        "active": True,
    },
    {
        "id": "SUP002",
        "name": "Industrial Parts",
        "tax_id": "00000000000201",
        "lead_time_days": 5,
        "payment_term_days": 30,
        "delivery_reliability": 0.88,
        "price_variability": 0.040,
        "active": True,
    },
    {
        "id": "SUP003",
        "name": "Lubrificantes Centro-Oeste",
        "tax_id": "00000000000301",
        "lead_time_days": 2,
        "payment_term_days": 21,
        "delivery_reliability": 0.96,
        "price_variability": 0.020,
        "active": True,
    },
    {
        "id": "SUP004",
        "name": "Automacao Industrial",
        "tax_id": "00000000000401",
        "lead_time_days": 7,
        "payment_term_days": 35,
        "delivery_reliability": 0.84,
        "price_variability": 0.060,
        "active": True,
    },
    {
        "id": "SUP005",
        "name": "MRO Suprimentos",
        "tax_id": "00000000000501",
        "lead_time_days": 4,
        "payment_term_days": 28,
        "delivery_reliability": 0.90,
        "price_variability": 0.035,
        "active": True,
    },
    {
        "id": "SUP006",
        "name": "Filtros & Vedacoes",
        "tax_id": "00000000000601",
        "lead_time_days": 3,
        "payment_term_days": 30,
        "delivery_reliability": 0.92,
        "price_variability": 0.030,
        "active": True,
    },
]

WAREHOUSES = [
    {"id": "ALM01", "name": "Almoxarifado Central", "active": True},
    {"id": "ALM02", "name": "Almoxarifado Manutencao", "active": True},
]

COST_CENTERS = [
    {"id": "CC-MEC", "name": "Manutencao Mecanica", "active": True},
    {"id": "CC-ELE", "name": "Manutencao Eletrica", "active": True},
    {"id": "CC-UTIL", "name": "Utilidades", "active": True},
    {"id": "CC-PROD", "name": "Producao", "active": True},
]

MATERIALS = [
    # id, description, category, uom, ncm, criticality, cents, reorder point,
    # reorder qty, supplier, ICMS/IPI/PIS/COFINS in basis points.
    ("ROL-6205", "Rolamento rigido 6205", "BEARING", "EA", "84821010", "HIGH", 5800, 8.0, 25.0, "SUP001", 1800, 500, 165, 760),
    ("ROL-6308", "Rolamento rigido 6308", "BEARING", "EA", "84821010", "HIGH", 12600, 5.0, 15.0, "SUP001", 1800, 500, 165, 760),
    ("COR-A42", "Correia industrial A42", "BELT", "EA", "40103200", "MEDIUM", 4200, 5.0, 15.0, "SUP002", 1800, 500, 165, 760),
    ("COR-B55", "Correia industrial B55", "BELT", "EA", "40103200", "MEDIUM", 7300, 4.0, 12.0, "SUP002", 1800, 500, 165, 760),
    ("OLEO-ISO68", "Oleo hidraulico ISO VG 68", "LUBRICANT", "L", "27101932", "HIGH", 1850, 30.0, 100.0, "SUP003", 1800, 0, 165, 760),
    ("GRAXA-EP2", "Graxa industrial EP2", "LUBRICANT", "KG", "27101999", "MEDIUM", 2650, 20.0, 60.0, "SUP003", 1800, 0, 165, 760),
    ("FILT-HYD01", "Filtro hidraulico", "FILTER", "EA", "84212990", "HIGH", 7900, 6.0, 20.0, "SUP006", 1800, 500, 165, 760),
    ("FILT-AR01", "Filtro de ar industrial", "FILTER", "EA", "84213100", "MEDIUM", 6100, 6.0, 20.0, "SUP006", 1800, 500, 165, 760),
    ("CONT-32A", "Contator tripolar 32A", "ELECTRICAL", "EA", "85364900", "HIGH", 14500, 4.0, 12.0, "SUP004", 1800, 500, 165, 760),
    ("SENS-IND", "Sensor indutivo M18", "ELECTRICAL", "EA", "85365090", "HIGH", 11800, 5.0, 15.0, "SUP004", 1800, 500, 165, 760),
    ("RET-35", "Retentor industrial 35 mm", "SEAL", "EA", "40169300", "MEDIUM", 2100, 10.0, 30.0, "SUP005", 1800, 500, 165, 760),
    ("PAR-M10", "Parafuso sextavado M10", "FASTENER", "EA", "73181500", "LOW", 230, 50.0, 200.0, "SUP005", 1800, 500, 165, 760),
]

ASSETS = [
    ("PUMP-001", "Bomba centrifuga linha 1", "PUMP", "CRITICAL", "CC-MEC", "ALM02"),
    ("PUMP-002", "Bomba centrifuga linha 2", "PUMP", "HIGH", "CC-MEC", "ALM02"),
    ("CONV-001", "Transportador de correia 1", "CONVEYOR", "HIGH", "CC-MEC", "ALM02"),
    ("CONV-002", "Transportador de correia 2", "CONVEYOR", "MEDIUM", "CC-MEC", "ALM02"),
    ("COMP-001", "Compressor de ar principal", "COMPRESSOR", "CRITICAL", "CC-UTIL", "ALM01"),
    ("HYD-001", "Unidade hidraulica prensa 1", "HYDRAULIC", "CRITICAL", "CC-MEC", "ALM02"),
    ("MCC-001", "Centro de controle de motores", "ELECTRICAL", "CRITICAL", "CC-ELE", "ALM01"),
    ("PACK-001", "Linha de embalagem", "PACKAGING", "HIGH", "CC-PROD", "ALM01"),
]

PROFILES: dict[str, list[tuple[str, float, float, float]]] = {
    "PUMP": [
        ("ROL-6205", 0.75, 1.0, 2.0),
        ("RET-35", 0.70, 1.0, 2.0),
        ("GRAXA-EP2", 0.55, 1.0, 3.0),
        ("PAR-M10", 0.35, 4.0, 16.0),
    ],
    "CONVEYOR": [
        ("COR-A42", 0.65, 1.0, 2.0),
        ("COR-B55", 0.40, 1.0, 2.0),
        ("ROL-6308", 0.55, 1.0, 2.0),
        ("PAR-M10", 0.50, 6.0, 20.0),
    ],
    "COMPRESSOR": [
        ("FILT-AR01", 0.75, 1.0, 3.0),
        ("OLEO-ISO68", 0.65, 10.0, 30.0),
        ("ROL-6308", 0.30, 1.0, 2.0),
        ("CONT-32A", 0.20, 1.0, 1.0),
    ],
    "HYDRAULIC": [
        ("FILT-HYD01", 0.80, 1.0, 2.0),
        ("OLEO-ISO68", 0.85, 10.0, 40.0),
        ("RET-35", 0.50, 1.0, 4.0),
        ("SENS-IND", 0.15, 1.0, 1.0),
    ],
    "ELECTRICAL": [
        ("CONT-32A", 0.70, 1.0, 2.0),
        ("SENS-IND", 0.60, 1.0, 3.0),
        ("PAR-M10", 0.20, 2.0, 8.0),
    ],
    "PACKAGING": [
        ("SENS-IND", 0.60, 1.0, 2.0),
        ("COR-A42", 0.50, 1.0, 2.0),
        ("ROL-6205", 0.45, 1.0, 2.0),
        ("PAR-M10", 0.45, 4.0, 12.0),
    ],
}


# ---------------------------------------------------------------------------
# State machines
# ---------------------------------------------------------------------------

ALLOWED_TRANSITIONS: dict[str, dict[str, set[str]]] = {
    "work_order": {
        "PLANNED": {"RELEASED"},
        "RELEASED": {"WAITING_MATERIAL", "IN_PROGRESS"},
        "WAITING_MATERIAL": {"IN_PROGRESS"},
        "IN_PROGRESS": {"COMPLETED"},
        "COMPLETED": {"CLOSED"},
        "CLOSED": set(),
    },
    "work_order_material": {
        "REQUESTED": {"PARTIALLY_ISSUED", "FULFILLED"},
        "PARTIALLY_ISSUED": {"FULFILLED"},
        "FULFILLED": set(),
    },
    "purchase_requisition": {
        "REQUESTED": {"APPROVED"},
        "APPROVED": {"ORDERED"},
        "ORDERED": {"CLOSED"},
        "CLOSED": set(),
    },
    "purchase_order": {
        "APPROVED": {"SENT"},
        "SENT": {"PARTIALLY_RECEIVED", "RECEIVED"},
        "PARTIALLY_RECEIVED": {"RECEIVED"},
        "RECEIVED": {"CLOSED"},
        "CLOSED": set(),
    },
    "fiscal_invoice": {
        "RECEIVED": {"VALIDATING"},
        "VALIDATING": {"BLOCKED", "MATCHED"},
        "BLOCKED": {"RELEASED"},
        "RELEASED": {"MATCHED"},
        "MATCHED": {"POSTED"},
        "POSTED": set(),
    },
    "accounts_payable": {
        "OPEN": {"SCHEDULED"},
        "SCHEDULED": {"PAID"},
        "PAID": set(),
    },
}

STATE_TABLE_FOR_ENTITY = {
    "work_order": "work_order_state",
    "work_order_material": "work_order_material_state",
    "purchase_requisition": "purchase_requisition_state",
    "purchase_order": "purchase_order_state",
    "fiscal_invoice": "fiscal_invoice_state",
    "accounts_payable": "accounts_payable_state",
}


# ---------------------------------------------------------------------------
# Spark schemas
# ---------------------------------------------------------------------------

S = StringType()
I = IntegerType()
L = LongType()
D = DoubleType()
B = BooleanType()
T = TimestampType()


def schema(*fields: tuple[str, Any, bool]) -> StructType:
    return StructType([StructField(name, dtype, nullable) for name, dtype, nullable in fields])


SCHEMAS: dict[str, StructType] = {
    "supplier": schema(
        ("id", S, False), ("name", S, False), ("tax_id", S, True),
        ("lead_time_days", I, False), ("payment_term_days", I, False),
        ("delivery_reliability", D, False), ("price_variability", D, False),
        ("active", B, False),
    ),
    "warehouse": schema(("id", S, False), ("name", S, False), ("active", B, False)),
    "cost_center": schema(("id", S, False), ("name", S, False), ("active", B, False)),
    "asset": schema(
        ("id", S, False), ("description", S, False), ("asset_type", S, False),
        ("criticality", S, False), ("cost_center_id", S, False),
        ("warehouse_id", S, False), ("active", B, False),
    ),
    "material": schema(
        ("id", S, False), ("description", S, False), ("category", S, False),
        ("uom", S, False), ("ncm", S, True), ("criticality", S, False),
        ("unit_cost_cents", L, False), ("reorder_point", D, False),
        ("reorder_qty", D, False), ("preferred_supplier_id", S, False),
        ("icms_bps", I, False), ("ipi_bps", I, False),
        ("pis_bps", I, False), ("cofins_bps", I, False), ("active", B, False),
    ),
    "asset_material_profile": schema(
        ("asset_id", S, False), ("material_id", S, False),
        ("probability", D, False), ("min_qty", D, False), ("max_qty", D, False),
    ),
    "sim_state": schema(
        ("id", S, False), ("simulated_at", T, False), ("committed_tick", L, False),
        ("seed", I, False), ("step_minutes", I, False),
        ("business_timezone", S, False), ("updated_at", T, False),
    ),
    "simulation_run": schema(
        ("run_id", S, False), ("started_at", T, False), ("finished_at", T, True),
        ("from_tick", L, False), ("to_tick", L, True), ("status", S, False),
        ("error", S, True),
    ),
    "stock_balance_state": schema(
        ("material_id", S, False), ("warehouse_id", S, False),
        ("on_hand", D, False), ("reserved", D, False),
        ("last_counted_at", T, True), ("valid_from_tick", L, False),
    ),
    "work_order_state": schema(
        ("id", S, False), ("order_no", S, False), ("asset_id", S, False),
        ("maintenance_type", S, False), ("priority", S, False),
        ("status", S, False), ("cost_center_id", S, False),
        ("warehouse_id", S, False), ("planned_at", T, False),
        ("release_after", T, False), ("released_at", T, True),
        ("started_at", T, True), ("complete_after", T, True),
        ("completed_at", T, True), ("close_after", T, True),
        ("closed_at", T, True), ("valid_from_tick", L, False),
    ),
    "work_order_material_state": schema(
        ("id", S, False), ("work_order_id", S, False), ("material_id", S, False),
        ("qty_required", D, False), ("qty_issued", D, False),
        ("status", S, False), ("valid_from_tick", L, False),
    ),
    "purchase_requisition_state": schema(
        ("id", S, False), ("request_no", S, False), ("warehouse_id", S, False),
        ("source_type", S, False), ("source_ref", S, False),
        ("priority", S, False), ("status", S, False),
        ("requested_at", T, False), ("approve_after", T, False),
        ("approved_at", T, True), ("valid_from_tick", L, False),
    ),
    "purchase_requisition_item_store": schema(
        ("id", S, False), ("requisition_id", S, False),
        ("material_id", S, False), ("quantity", D, False),
        ("simulation_tick", L, False),
    ),
    "purchase_order_state": schema(
        ("id", S, False), ("po_no", S, False), ("requisition_id", S, False),
        ("supplier_id", S, False), ("warehouse_id", S, False),
        ("status", S, False), ("approved_at", T, False),
        ("send_after", T, False), ("sent_at", T, True),
        ("promised_at", T, False), ("next_receipt_at", T, False),
        ("received_at", T, True), ("closed_at", T, True),
        ("receipt_count", I, False), ("valid_from_tick", L, False),
    ),
    "purchase_order_item_state": schema(
        ("id", S, False), ("purchase_order_id", S, False),
        ("requisition_item_id", S, False), ("material_id", S, False),
        ("quantity", D, False), ("unit_price_cents", L, False),
        ("received_qty", D, False), ("valid_from_tick", L, False),
    ),
    "goods_receipt_store": schema(
        ("id", S, False), ("receipt_no", S, False),
        ("purchase_order_id", S, False), ("warehouse_id", S, False),
        ("received_at", T, False), ("simulation_tick", L, False),
    ),
    "goods_receipt_item_store": schema(
        ("id", S, False), ("goods_receipt_id", S, False),
        ("purchase_order_item_id", S, False), ("material_id", S, False),
        ("qty_received", D, False), ("simulation_tick", L, False),
    ),
    "inventory_movement_store": schema(
        ("id", S, False), ("material_id", S, False), ("warehouse_id", S, False),
        ("movement_type", S, False), ("quantity", D, False),
        ("unit_cost_cents", L, False), ("reference_type", S, True),
        ("reference_id", S, True), ("occurred_at", T, False),
        ("simulation_tick", L, False),
    ),
    "fiscal_invoice_state": schema(
        ("id", S, False), ("invoice_no", S, False), ("supplier_id", S, False),
        ("purchase_order_id", S, False), ("goods_receipt_id", S, False),
        ("status", S, False), ("issued_at", T, False),
        ("received_at", T, False), ("validate_after", T, False),
        ("validation_complete_after", T, True), ("blocked_until", T, True),
        ("subtotal_cents", L, False), ("tax_total_cents", L, False),
        ("total_amount_cents", L, False), ("price_variance_bps", I, False),
        ("tax_issue", B, False), ("valid_from_tick", L, False),
    ),
    "fiscal_invoice_item_store": schema(
        ("id", S, False), ("invoice_id", S, False), ("material_id", S, False),
        ("quantity", D, False), ("unit_price_cents", L, False),
        ("icms_cents", L, False), ("ipi_cents", L, False),
        ("pis_cents", L, False), ("cofins_cents", L, False),
        ("simulation_tick", L, False),
    ),
    "accounts_payable_state": schema(
        ("id", S, False), ("invoice_id", S, False), ("supplier_id", S, False),
        ("status", S, False), ("amount_cents", L, False),
        ("posted_at", T, False), ("due_at", T, False),
        ("scheduled_at", T, True), ("payment_scheduled_for", T, True),
        ("paid_at", T, True), ("valid_from_tick", L, False),
    ),
    "payment_store": schema(
        ("id", S, False), ("payment_no", S, False),
        ("accounts_payable_id", S, False), ("amount_cents", L, False),
        ("paid_at", T, False), ("simulation_tick", L, False),
    ),
    "entity_state_transition_store": schema(
        ("id", S, False), ("entity_type", S, False), ("entity_id", S, False),
        ("from_state", S, True), ("to_state", S, False), ("reason", S, True),
        ("occurred_at", T, False), ("simulation_tick", L, False),
    ),
    "event_log_store": schema(
        ("id", S, False), ("event_type", S, False),
        ("entity_type", S, True), ("entity_id", S, True),
        ("occurred_at", T, False), ("payload", S, False),
        ("simulation_tick", L, False),
    ),
}

STATE_TABLE_KEYS: dict[str, list[str]] = {
    "stock_balance_state": ["material_id", "warehouse_id", "valid_from_tick"],
    "work_order_state": ["id", "valid_from_tick"],
    "work_order_material_state": ["id", "valid_from_tick"],
    "purchase_requisition_state": ["id", "valid_from_tick"],
    "purchase_order_state": ["id", "valid_from_tick"],
    "purchase_order_item_state": ["id", "valid_from_tick"],
    "fiscal_invoice_state": ["id", "valid_from_tick"],
    "accounts_payable_state": ["id", "valid_from_tick"],
}

APPEND_TABLE_KEYS: dict[str, list[str]] = {
    "purchase_requisition_item_store": ["id"],
    "goods_receipt_store": ["id"],
    "goods_receipt_item_store": ["id"],
    "inventory_movement_store": ["id"],
    "fiscal_invoice_item_store": ["id"],
    "payment_store": ["id"],
    "entity_state_transition_store": ["id"],
    "event_log_store": ["id"],
    "simulation_run": ["run_id"],
}

CURRENT_VIEWS = {
    "stock_balance": ("stock_balance_state", ["material_id", "warehouse_id"]),
    "work_order": ("work_order_state", ["id"]),
    "work_order_material": ("work_order_material_state", ["id"]),
    "purchase_requisition": ("purchase_requisition_state", ["id"]),
    "purchase_order": ("purchase_order_state", ["id"]),
    "purchase_order_item": ("purchase_order_item_state", ["id"]),
    "fiscal_invoice": ("fiscal_invoice_state", ["id"]),
    "accounts_payable": ("accounts_payable_state", ["id"]),
}

COMMITTED_VIEWS = {
    "purchase_requisition_item": "purchase_requisition_item_store",
    "goods_receipt": "goods_receipt_store",
    "goods_receipt_item": "goods_receipt_item_store",
    "inventory_movement": "inventory_movement_store",
    "fiscal_invoice_item": "fiscal_invoice_item_store",
    "payment": "payment_store",
    "entity_state_transition": "entity_state_transition_store",
    "event_log": "event_log_store",
}


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def validate_identifier(value: str, label: str) -> str:
    """
    Validate a Unity Catalog securable identifier.

    Unity Catalog allows characters such as hyphens when the identifier is
    referenced with backticks. Catalog/schema/object names cannot contain:
    - period (.)
    - space
    - forward slash (/)
    - ASCII control characters (0x00-0x1F)
    - DELETE (0x7F)

    The SQL-building code quotes catalog/schema/object names with backticks,
    so values such as ``mro-data`` are valid.
    """
    if not value:
        raise ValueError(f"{label} identifier cannot be empty")

    if len(value) > 255:
        raise ValueError(
            f"{label} identifier exceeds Unity Catalog's 255-character limit: {value!r}"
        )

    forbidden = {".", " ", "/", "`"}
    if any(ch in forbidden or ord(ch) < 32 or ord(ch) == 127 for ch in value):
        raise ValueError(
            f"Invalid {label} identifier for Unity Catalog: {value!r}. "
            "Periods, spaces, forward slashes, backticks, and ASCII control "
            "characters are not allowed by this simulator adapter."
        )

    # SQL references are emitted with backtick quoting by Config.namespace/fq.
    return value


def parse_timestamp(value: str) -> datetime:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def stable_id(*parts: Any, length: int = 24) -> str:
    raw = "|".join(str(part) for part in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:length]


def poisson(rng: random.Random, lmbda: float) -> int:
    if lmbda <= 0:
        return 0
    limit = math.exp(-lmbda)
    k = 0
    p = 1.0
    while p > limit:
        k += 1
        p *= rng.random()
    return k - 1


def tax_amount(base_cents: int, bps: int) -> int:
    return round(base_cents * bps / 10_000)


def money_with_variance(base_cents: int, rng: random.Random, variability: float) -> int:
    return max(1, round(base_cents * (1.0 + rng.uniform(-variability, variability))))


def as_dict(row: Row) -> dict[str, Any]:
    return row.asDict(recursive=True)


@dataclass
class Config:
    catalog: str
    schema: str
    start: datetime
    step_minutes: int
    ticks_per_run: int
    commit_every_ticks: int
    seed: int
    work_orders_per_day: float
    cycle_counts_per_day: float
    business_timezone: str
    reset: bool
    bootstrap_only: bool

    @property
    def namespace(self) -> str:
        return f"`{self.catalog}`.`{self.schema}`"

    def fq(self, object_name: str) -> str:
        return f"{self.namespace}.`{object_name}`"


# ---------------------------------------------------------------------------
# Delta persistence / Unity Catalog bootstrap
# ---------------------------------------------------------------------------

class DeltaStore:
    def __init__(self, spark: SparkSession, config: Config):
        self.spark = spark
        self.config = config
        self.temp_counter = 0

    def fq(self, table: str) -> str:
        return self.config.fq(table)

    def table_exists(self, table: str) -> bool:
        return self.spark.catalog.tableExists(f"{self.config.catalog}.{self.config.schema}.{table}")

    def bootstrap_schema(self) -> None:
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.config.namespace}")
        for name, spark_schema in SCHEMAS.items():
            ddl = ",\n  ".join(
                f"`{field.name}` {field.dataType.simpleString().upper()}"
                for field in spark_schema.fields
            )
            self.spark.sql(
                f"CREATE TABLE IF NOT EXISTS {self.fq(name)} (\n  {ddl}\n) USING DELTA"
            )
        self.create_views()

    def create_views(self) -> None:
        committed_tick = f"(SELECT committed_tick FROM {self.fq('sim_state')} WHERE id = 'main')"

        for view_name, (state_table, partition_cols) in CURRENT_VIEWS.items():
            partition = ", ".join(f"`{c}`" for c in partition_cols)
            self.spark.sql(
                f"""
                CREATE OR REPLACE VIEW {self.fq(view_name)} AS
                SELECT * EXCEPT (__rn)
                FROM (
                    SELECT s.*,
                           ROW_NUMBER() OVER (
                               PARTITION BY {partition}
                               ORDER BY valid_from_tick DESC
                           ) AS __rn
                    FROM {self.fq(state_table)} s
                    WHERE valid_from_tick <= {committed_tick}
                )
                WHERE __rn = 1
                """
            )

            self.spark.sql(
                f"""
                CREATE OR REPLACE VIEW {self.fq(view_name + '_history')} AS
                SELECT *
                FROM {self.fq(state_table)}
                WHERE valid_from_tick <= {committed_tick}
                """
            )

        for view_name, store_table in COMMITTED_VIEWS.items():
            self.spark.sql(
                f"""
                CREATE OR REPLACE VIEW {self.fq(view_name)} AS
                SELECT *
                FROM {self.fq(store_table)}
                WHERE simulation_tick <= {committed_tick}
                """
            )

        # Useful operational views.
        self.spark.sql(
            f"""
            CREATE OR REPLACE VIEW {self.fq('v_stock_position')} AS
            SELECT
                sb.material_id,
                m.description,
                sb.warehouse_id,
                sb.on_hand,
                sb.reserved,
                sb.on_hand - sb.reserved AS available,
                COALESCE(po.on_order, 0.0) AS on_order,
                m.reorder_point,
                m.reorder_qty
            FROM {self.fq('stock_balance')} sb
            JOIN {self.fq('material')} m ON m.id = sb.material_id
            LEFT JOIN (
                SELECT
                    poi.material_id,
                    po.warehouse_id,
                    SUM(poi.quantity - poi.received_qty) AS on_order
                FROM {self.fq('purchase_order_item')} poi
                JOIN {self.fq('purchase_order')} po ON po.id = poi.purchase_order_id
                WHERE po.status IN ('APPROVED', 'SENT', 'PARTIALLY_RECEIVED')
                GROUP BY poi.material_id, po.warehouse_id
            ) po
              ON po.material_id = sb.material_id
             AND po.warehouse_id = sb.warehouse_id
            """
        )

        self.spark.sql(
            f"""
            CREATE OR REPLACE VIEW {self.fq('v_open_ap')} AS
            SELECT
                ap.*,
                fi.invoice_no,
                DATEDIFF(CAST((SELECT simulated_at FROM {self.fq('sim_state')} WHERE id = 'main') AS DATE), CAST(ap.due_at AS DATE)) AS days_past_due
            FROM {self.fq('accounts_payable')} ap
            JOIN {self.fq('fiscal_invoice')} fi ON fi.id = ap.invoice_id
            WHERE ap.status <> 'PAID'
            """
        )

    def merge_rows(self, table: str, rows: list[dict[str, Any]], keys: list[str]) -> None:
        if not rows:
            return
        spark_schema = SCHEMAS[table]
        normalized = []
        names = [field.name for field in spark_schema.fields]
        for row in rows:
            normalized.append({name: row.get(name) for name in names})

        df = self.spark.createDataFrame(normalized, schema=spark_schema)
        self.temp_counter += 1
        temp_view = f"_mro_merge_{table}_{self.temp_counter}"
        df.createOrReplaceTempView(temp_view)
        condition = " AND ".join(f"t.`{key}` = s.`{key}`" for key in keys)
        self.spark.sql(
            f"""
            MERGE INTO {self.fq(table)} t
            USING {temp_view} s
            ON {condition}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """
        )
        self.spark.catalog.dropTempView(temp_view)

    def insert_only(self, table: str, rows: list[dict[str, Any]], keys: list[str]) -> None:
        if not rows:
            return
        spark_schema = SCHEMAS[table]
        names = [field.name for field in spark_schema.fields]
        normalized = [{name: row.get(name) for name in names} for row in rows]
        df = self.spark.createDataFrame(normalized, schema=spark_schema)
        self.temp_counter += 1
        temp_view = f"_mro_insert_{table}_{self.temp_counter}"
        df.createOrReplaceTempView(temp_view)
        condition = " AND ".join(f"t.`{key}` = s.`{key}`" for key in keys)
        self.spark.sql(
            f"""
            MERGE INTO {self.fq(table)} t
            USING {temp_view} s
            ON {condition}
            WHEN NOT MATCHED THEN INSERT *
            """
        )
        self.spark.catalog.dropTempView(temp_view)

    def replace_run(self, row: dict[str, Any]) -> None:
        self.merge_rows("simulation_run", [row], ["run_id"])

    def load_rows(self, object_name: str) -> list[dict[str, Any]]:
        return [as_dict(row) for row in self.spark.table(f"{self.config.catalog}.{self.config.schema}.{object_name}").collect()]

    def load_state(self) -> dict[str, Any] | None:
        rows = self.spark.table(f"{self.config.catalog}.{self.config.schema}.sim_state").where("id = 'main'").collect()
        return as_dict(rows[0]) if rows else None

    def commit_sim_state(self, simulated_at: datetime, tick: int) -> None:
        current = self.load_state()
        row = {
            "id": "main",
            "simulated_at": simulated_at,
            "committed_tick": int(tick),
            "seed": self.config.seed if current is None else int(current["seed"]),
            "step_minutes": self.config.step_minutes if current is None else int(current["step_minutes"]),
            "business_timezone": self.config.business_timezone if current is None else current["business_timezone"],
            "updated_at": utc_now(),
        }
        self.merge_rows("sim_state", [row], ["id"])


# ---------------------------------------------------------------------------
# In-memory simulation engine
# ---------------------------------------------------------------------------

class SimulationEngine:
    def __init__(self, store: DeltaStore, config: Config):
        self.store = store
        self.config = config
        self.business_tz = ZoneInfo(config.business_timezone)

        # Master/reference data.
        self.suppliers: dict[str, dict[str, Any]] = {}
        self.warehouses: dict[str, dict[str, Any]] = {}
        self.cost_centers: dict[str, dict[str, Any]] = {}
        self.materials: dict[str, dict[str, Any]] = {}
        self.assets: dict[str, dict[str, Any]] = {}
        self.profiles_by_asset: dict[str, list[dict[str, Any]]] = defaultdict(list)

        # Current operational state.
        self.stock: dict[tuple[str, str], dict[str, Any]] = {}
        self.work_orders: dict[str, dict[str, Any]] = {}
        self.work_order_materials: dict[str, dict[str, Any]] = {}
        self.purchase_requisitions: dict[str, dict[str, Any]] = {}
        self.requisition_items: dict[str, dict[str, Any]] = {}
        self.purchase_orders: dict[str, dict[str, Any]] = {}
        self.purchase_order_items: dict[str, dict[str, Any]] = {}
        self.invoices: dict[str, dict[str, Any]] = {}
        self.accounts_payable: dict[str, dict[str, Any]] = {}

        # Pending durable writes since the last commit boundary.
        self.pending_state: dict[str, dict[tuple[Any, ...], dict[str, Any]]] = defaultdict(dict)
        self.pending_append: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
        self.event_counter = 0
        self.movement_counter = 0

    # ---- bootstrap and loading ---------------------------------------------

    def seed_master_data(self) -> None:
        material_rows = []
        for row in MATERIALS:
            (
                material_id, description, category, uom, ncm, criticality,
                unit_cost_cents, reorder_point, reorder_qty, supplier_id,
                icms_bps, ipi_bps, pis_bps, cofins_bps,
            ) = row
            material_rows.append(
                {
                    "id": material_id,
                    "description": description,
                    "category": category,
                    "uom": uom,
                    "ncm": ncm,
                    "criticality": criticality,
                    "unit_cost_cents": int(unit_cost_cents),
                    "reorder_point": float(reorder_point),
                    "reorder_qty": float(reorder_qty),
                    "preferred_supplier_id": supplier_id,
                    "icms_bps": int(icms_bps),
                    "ipi_bps": int(ipi_bps),
                    "pis_bps": int(pis_bps),
                    "cofins_bps": int(cofins_bps),
                    "active": True,
                }
            )

        asset_rows = [
            {
                "id": asset_id,
                "description": description,
                "asset_type": asset_type,
                "criticality": criticality,
                "cost_center_id": cc,
                "warehouse_id": wh,
                "active": True,
            }
            for asset_id, description, asset_type, criticality, cc, wh in ASSETS
        ]

        profile_rows = []
        asset_types = {a[0]: a[2] for a in ASSETS}
        for asset_id, asset_type in asset_types.items():
            for material_id, probability, min_qty, max_qty in PROFILES[asset_type]:
                profile_rows.append(
                    {
                        "asset_id": asset_id,
                        "material_id": material_id,
                        "probability": float(probability),
                        "min_qty": float(min_qty),
                        "max_qty": float(max_qty),
                    }
                )

        self.store.merge_rows("supplier", SUPPLIERS, ["id"])
        self.store.merge_rows("warehouse", WAREHOUSES, ["id"])
        self.store.merge_rows("cost_center", COST_CENTERS, ["id"])
        self.store.merge_rows("material", material_rows, ["id"])
        self.store.merge_rows("asset", asset_rows, ["id"])
        self.store.merge_rows("asset_material_profile", profile_rows, ["asset_id", "material_id"])

    def bootstrap_state_if_needed(self) -> None:
        state = self.store.load_state()
        is_new = state is None
        if state is not None:
            if int(state["seed"]) != self.config.seed:
                raise ValueError(
                    f"Existing simulation uses seed={state['seed']}, requested seed={self.config.seed}. "
                    "Use another schema or --reset."
                )
            if int(state["step_minutes"]) != self.config.step_minutes:
                raise ValueError(
                    f"Existing simulation uses step_minutes={state['step_minutes']}, requested "
                    f"{self.config.step_minutes}. Use another schema or --reset."
                )
            if state["business_timezone"] != self.config.business_timezone:
                raise ValueError(
                    f"Existing simulation uses timezone={state['business_timezone']}, requested "
                    f"{self.config.business_timezone}. Use another schema or --reset."
                )

        # Bootstrap rows are deterministic and MERGEd. Re-running this block is
        # therefore safe and repairs an interrupted first bootstrap before the
        # simulation starts advancing.
        rng = random.Random(self.config.seed)
        initial_stock_rows: list[dict[str, Any]] = []
        initial_movements: list[dict[str, Any]] = []

        material_by_id = {row[0]: row for row in MATERIALS}
        for material_id, material in material_by_id.items():
            reorder_point = float(material[7])
            unit_cost_cents = int(material[6])
            for warehouse in WAREHOUSES:
                warehouse_id = warehouse["id"]
                qty = round(rng.uniform(max(1.0, reorder_point * 0.60), reorder_point * 2.20), 3)
                initial_stock_rows.append(
                    {
                        "material_id": material_id,
                        "warehouse_id": warehouse_id,
                        "on_hand": qty,
                        "reserved": 0.0,
                        "last_counted_at": self.config.start,
                        "valid_from_tick": 0,
                    }
                )
                initial_movements.append(
                    {
                        "id": stable_id("initial-stock", material_id, warehouse_id),
                        "material_id": material_id,
                        "warehouse_id": warehouse_id,
                        "movement_type": "INITIAL_BALANCE",
                        "quantity": qty,
                        "unit_cost_cents": unit_cost_cents,
                        "reference_type": "SIMULATION",
                        "reference_id": "BOOTSTRAP",
                        "occurred_at": self.config.start,
                        "simulation_tick": 0,
                    }
                )

        self.store.merge_rows(
            "stock_balance_state",
            initial_stock_rows,
            STATE_TABLE_KEYS["stock_balance_state"],
        )
        self.store.insert_only(
            "inventory_movement_store",
            initial_movements,
            APPEND_TABLE_KEYS["inventory_movement_store"],
        )
        if is_new:
            # Visibility marker is written only after initial state exists.
            self.store.commit_sim_state(self.config.start, 0)

    def load(self) -> tuple[datetime, int]:
        self.suppliers = {r["id"]: r for r in self.store.load_rows("supplier")}
        self.warehouses = {r["id"]: r for r in self.store.load_rows("warehouse")}
        self.cost_centers = {r["id"]: r for r in self.store.load_rows("cost_center")}
        self.materials = {r["id"]: r for r in self.store.load_rows("material")}
        self.assets = {r["id"]: r for r in self.store.load_rows("asset")}

        for row in self.store.load_rows("asset_material_profile"):
            self.profiles_by_asset[row["asset_id"]].append(row)

        self.stock = {
            (r["material_id"], r["warehouse_id"]): self._strip_version(r)
            for r in self.store.load_rows("stock_balance")
        }
        self.work_orders = {r["id"]: self._strip_version(r) for r in self.store.load_rows("work_order")}
        self.work_order_materials = {
            r["id"]: self._strip_version(r) for r in self.store.load_rows("work_order_material")
        }
        self.purchase_requisitions = {
            r["id"]: self._strip_version(r) for r in self.store.load_rows("purchase_requisition")
        }
        self.requisition_items = {
            r["id"]: r for r in self.store.load_rows("purchase_requisition_item")
        }
        self.purchase_orders = {
            r["id"]: self._strip_version(r) for r in self.store.load_rows("purchase_order")
        }
        self.purchase_order_items = {
            r["id"]: self._strip_version(r) for r in self.store.load_rows("purchase_order_item")
        }
        self.invoices = {
            r["id"]: self._strip_version(r) for r in self.store.load_rows("fiscal_invoice")
        }
        self.accounts_payable = {
            r["id"]: self._strip_version(r) for r in self.store.load_rows("accounts_payable")
        }

        state = self.store.load_state()
        if state is None:
            raise RuntimeError("sim_state was not initialized")
        return state["simulated_at"], int(state["committed_tick"])

    @staticmethod
    def _strip_version(row: dict[str, Any]) -> dict[str, Any]:
        row = dict(row)
        row.pop("valid_from_tick", None)
        row.pop("__rn", None)
        return row

    # ---- pending writes -----------------------------------------------------

    def mark_state(self, table: str, row: dict[str, Any], tick: int) -> None:
        persisted = dict(row)
        persisted["valid_from_tick"] = int(tick)
        logical_keys = [key for key in STATE_TABLE_KEYS[table] if key != "valid_from_tick"]
        key = tuple(persisted[k] for k in logical_keys) + (int(tick),)
        self.pending_state[table][key] = persisted

    def append(self, table: str, row: dict[str, Any]) -> None:
        self.pending_append[table][row["id"]] = row

    def transition(
        self,
        entity_type: str,
        entity: dict[str, Any],
        new_state: str,
        now: datetime,
        tick: int,
        *,
        reason: str | None = None,
        extra: dict[str, Any] | None = None,
    ) -> None:
        old_state = entity["status"]
        if old_state == new_state:
            return
        if new_state not in ALLOWED_TRANSITIONS[entity_type].get(old_state, set()):
            raise ValueError(
                f"Invalid transition {entity_type} {entity['id']}: {old_state} -> {new_state}"
            )
        entity["status"] = new_state
        if extra:
            entity.update(extra)
        self.mark_state(STATE_TABLE_FOR_ENTITY[entity_type], entity, tick)

        transition_id = stable_id(
            "transition", entity_type, entity["id"], old_state, new_state, tick, reason or ""
        )
        self.append(
            "entity_state_transition_store",
            {
                "id": transition_id,
                "entity_type": entity_type,
                "entity_id": entity["id"],
                "from_state": old_state,
                "to_state": new_state,
                "reason": reason,
                "occurred_at": now,
                "simulation_tick": tick,
            },
        )
        self.event(
            now,
            tick,
            f"{entity_type.upper()}_STATE_CHANGED",
            entity_type,
            entity["id"],
            {"from": old_state, "to": new_state, "reason": reason},
        )

    def record_initial_state(
        self,
        entity_type: str,
        entity: dict[str, Any],
        now: datetime,
        tick: int,
    ) -> None:
        self.mark_state(STATE_TABLE_FOR_ENTITY[entity_type], entity, tick)
        transition_id = stable_id(
            "transition", entity_type, entity["id"], "NULL", entity["status"], tick
        )
        self.append(
            "entity_state_transition_store",
            {
                "id": transition_id,
                "entity_type": entity_type,
                "entity_id": entity["id"],
                "from_state": None,
                "to_state": entity["status"],
                "reason": "created",
                "occurred_at": now,
                "simulation_tick": tick,
            },
        )

    def event(
        self,
        now: datetime,
        tick: int,
        event_type: str,
        entity_type: str | None = None,
        entity_id: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        self.event_counter += 1
        event_id = stable_id(
            "event", tick, self.event_counter, event_type, entity_type or "", entity_id or ""
        )
        self.append(
            "event_log_store",
            {
                "id": event_id,
                "event_type": event_type,
                "entity_type": entity_type,
                "entity_id": entity_id,
                "occurred_at": now,
                "payload": json.dumps(payload or {}, sort_keys=True, separators=(",", ":")),
                "simulation_tick": tick,
            },
        )

    def movement(
        self,
        *,
        now: datetime,
        tick: int,
        material_id: str,
        warehouse_id: str,
        movement_type: str,
        quantity: float,
        unit_cost_cents: int,
        reference_type: str,
        reference_id: str,
    ) -> None:
        self.movement_counter += 1
        movement_id = stable_id(
            "movement",
            tick,
            self.movement_counter,
            material_id,
            warehouse_id,
            reference_type,
            reference_id,
        )
        self.append(
            "inventory_movement_store",
            {
                "id": movement_id,
                "material_id": material_id,
                "warehouse_id": warehouse_id,
                "movement_type": movement_type,
                "quantity": float(quantity),
                "unit_cost_cents": int(unit_cost_cents),
                "reference_type": reference_type,
                "reference_id": reference_id,
                "occurred_at": now,
                "simulation_tick": tick,
            },
        )

    def flush(self, simulated_at: datetime, tick: int) -> None:
        # Versioned state first. It remains invisible through current views until
        # sim_state.committed_tick is advanced at the end of this method.
        for table, keyed_rows in self.pending_state.items():
            self.store.merge_rows(table, list(keyed_rows.values()), STATE_TABLE_KEYS[table])

        for table, keyed_rows in self.pending_append.items():
            self.store.insert_only(table, list(keyed_rows.values()), APPEND_TABLE_KEYS[table])

        # Commit marker last: this is the visibility boundary for readers.
        self.store.commit_sim_state(simulated_at, tick)
        self.pending_state.clear()
        self.pending_append.clear()

    # ---- business-time helpers ---------------------------------------------

    def to_business(self, dt: datetime) -> datetime:
        return dt.replace(tzinfo=timezone.utc).astimezone(self.business_tz)

    def from_business(self, dt: datetime) -> datetime:
        return dt.astimezone(timezone.utc).replace(tzinfo=None)

    def is_business_time(self, now: datetime) -> bool:
        local = self.to_business(now)
        return local.weekday() < 5 and 8 <= local.hour < 18

    def next_business_time(self, now: datetime, hour: int = 8) -> datetime:
        local = self.to_business(now)
        if local.weekday() < 5 and local.hour < 18:
            if local.hour < hour:
                local = local.replace(hour=hour, minute=0, second=0, microsecond=0)
            return self.from_business(local)
        local = local + timedelta(days=1)
        while local.weekday() >= 5:
            local += timedelta(days=1)
        local = local.replace(hour=hour, minute=0, second=0, microsecond=0)
        return self.from_business(local)

    # ---- domain behavior ----------------------------------------------------

    def create_work_orders(self, now: datetime, tick: int, rng: random.Random) -> None:
        count = poisson(rng, self.config.work_orders_per_day * self.config.step_minutes / 1440.0)
        active_assets = [a for a in self.assets.values() if a["active"]]

        for sequence in range(count):
            asset = rng.choice(active_assets)
            corrective_probability = {
                "LOW": 0.20,
                "MEDIUM": 0.25,
                "HIGH": 0.35,
                "CRITICAL": 0.45,
            }[asset["criticality"]]
            maintenance_type = "CORRECTIVE" if rng.random() < corrective_probability else "PREVENTIVE"

            if maintenance_type == "CORRECTIVE":
                priority = rng.choices(["NORMAL", "HIGH", "CRITICAL"], weights=[35, 45, 20])[0]
                release_delay = rng.uniform(0.0, 1.5)
            else:
                priority = rng.choices(["LOW", "NORMAL", "HIGH"], weights=[20, 70, 10])[0]
                release_delay = rng.uniform(4.0, 36.0)

            wo_id = stable_id("work_order", tick, sequence)
            order_no = f"WO-{tick:010d}-{sequence:03d}"
            wo = {
                "id": wo_id,
                "order_no": order_no,
                "asset_id": asset["id"],
                "maintenance_type": maintenance_type,
                "priority": priority,
                "status": "PLANNED",
                "cost_center_id": asset["cost_center_id"],
                "warehouse_id": asset["warehouse_id"],
                "planned_at": now,
                "release_after": now + timedelta(hours=release_delay),
                "released_at": None,
                "started_at": None,
                "complete_after": None,
                "completed_at": None,
                "close_after": None,
                "closed_at": None,
            }
            self.work_orders[wo_id] = wo
            self.record_initial_state("work_order", wo, now, tick)

            profiles = self.profiles_by_asset[asset["id"]]
            selected = [p for p in profiles if rng.random() <= p["probability"]]
            if not selected:
                selected = [rng.choice(profiles)]

            for profile in selected:
                material = self.materials[profile["material_id"]]
                if material["uom"] in ("L", "KG"):
                    quantity = round(rng.uniform(profile["min_qty"], profile["max_qty"]), 1)
                else:
                    quantity = float(rng.randint(math.ceil(profile["min_qty"]), math.floor(profile["max_qty"])))
                wom_id = stable_id("work_order_material", wo_id, material["id"])
                wom = {
                    "id": wom_id,
                    "work_order_id": wo_id,
                    "material_id": material["id"],
                    "qty_required": quantity,
                    "qty_issued": 0.0,
                    "status": "REQUESTED",
                }
                self.work_order_materials[wom_id] = wom
                self.record_initial_state("work_order_material", wom, now, tick)

            self.event(
                now,
                tick,
                "WORK_ORDER_CREATED",
                "work_order",
                wo_id,
                {
                    "asset_id": asset["id"],
                    "maintenance_type": maintenance_type,
                    "priority": priority,
                },
            )

    def release_work_orders(self, now: datetime, tick: int) -> None:
        for wo in list(self.work_orders.values()):
            if wo["status"] != "PLANNED" or wo["release_after"] > now:
                continue
            if wo["maintenance_type"] != "CORRECTIVE" and not self.is_business_time(now):
                continue
            self.transition(
                "work_order",
                wo,
                "RELEASED",
                now,
                tick,
                reason="maintenance order released",
                extra={"released_at": now},
            )

    def ensure_requisition(
        self,
        *,
        now: datetime,
        tick: int,
        rng: random.Random,
        warehouse_id: str,
        material_id: str,
        quantity: float,
        source_type: str,
        source_ref: str,
        priority: str,
    ) -> str:
        for pr in self.purchase_requisitions.values():
            if (
                pr["warehouse_id"] == warehouse_id
                and pr["source_type"] == source_type
                and pr["source_ref"] == source_ref
                and pr["status"] in {"REQUESTED", "APPROVED", "ORDERED"}
            ):
                item = next(
                    (
                        x for x in self.requisition_items.values()
                        if x["requisition_id"] == pr["id"] and x["material_id"] == material_id
                    ),
                    None,
                )
                if item:
                    return pr["id"]

        approval_hours = {
            "CRITICAL": rng.uniform(0.5, 2.0),
            "HIGH": rng.uniform(1.0, 5.0),
            "NORMAL": rng.uniform(4.0, 16.0),
        }[priority]
        approve_after = self.next_business_time(now + timedelta(hours=approval_hours))
        pr_id = stable_id("purchase_requisition", tick, source_type, source_ref, material_id, warehouse_id)
        pr = {
            "id": pr_id,
            "request_no": f"PR-{pr_id[:12].upper()}",
            "warehouse_id": warehouse_id,
            "source_type": source_type,
            "source_ref": source_ref,
            "priority": priority,
            "status": "REQUESTED",
            "requested_at": now,
            "approve_after": approve_after,
            "approved_at": None,
        }
        self.purchase_requisitions[pr_id] = pr
        self.record_initial_state("purchase_requisition", pr, now, tick)

        pri_id = stable_id("purchase_requisition_item", pr_id, material_id)
        pri = {
            "id": pri_id,
            "requisition_id": pr_id,
            "material_id": material_id,
            "quantity": float(round(quantity, 3)),
            "simulation_tick": tick,
        }
        self.requisition_items[pri_id] = pri
        self.append("purchase_requisition_item_store", pri)
        self.event(
            now,
            tick,
            "PURCHASE_REQUISITION_CREATED",
            "purchase_requisition",
            pr_id,
            {
                "material_id": material_id,
                "quantity": quantity,
                "source_type": source_type,
                "source_ref": source_ref,
                "priority": priority,
            },
        )
        return pr_id

    def issue_pending_materials(self, now: datetime, tick: int, rng: random.Random) -> None:
        touched: set[str] = set()
        candidates = sorted(self.work_order_materials.values(), key=lambda row: row["id"])

        for wom in candidates:
            wo = self.work_orders[wom["work_order_id"]]
            if wo["status"] not in {"RELEASED", "WAITING_MATERIAL"} or wom["status"] == "FULFILLED":
                continue

            missing = max(0.0, wom["qty_required"] - wom["qty_issued"])
            if missing <= 1e-9:
                continue

            stock_key = (wom["material_id"], wo["warehouse_id"])
            balance = self.stock[stock_key]
            issue_qty = min(balance["on_hand"], missing)

            if issue_qty > 1e-9:
                balance["on_hand"] = round(balance["on_hand"] - issue_qty, 6)
                self.mark_state("stock_balance_state", balance, tick)
                material = self.materials[wom["material_id"]]
                self.movement(
                    now=now,
                    tick=tick,
                    material_id=wom["material_id"],
                    warehouse_id=wo["warehouse_id"],
                    movement_type="ISSUE",
                    quantity=-issue_qty,
                    unit_cost_cents=material["unit_cost_cents"],
                    reference_type="WORK_ORDER",
                    reference_id=wo["id"],
                )
                wom["qty_issued"] = round(wom["qty_issued"] + issue_qty, 6)
                missing = max(0.0, wom["qty_required"] - wom["qty_issued"])

                if missing <= 1e-9:
                    self.transition(
                        "work_order_material",
                        wom,
                        "FULFILLED",
                        now,
                        tick,
                        reason="required quantity fully issued",
                    )
                elif wom["status"] == "REQUESTED":
                    self.transition(
                        "work_order_material",
                        wom,
                        "PARTIALLY_ISSUED",
                        now,
                        tick,
                        reason="partial stock issue",
                    )
                else:
                    self.mark_state("work_order_material_state", wom, tick)

            if missing > 1e-9:
                priority = "CRITICAL" if wo["priority"] == "CRITICAL" else "HIGH" if wo["priority"] == "HIGH" else "NORMAL"
                self.ensure_requisition(
                    now=now,
                    tick=tick,
                    rng=rng,
                    warehouse_id=wo["warehouse_id"],
                    material_id=wom["material_id"],
                    quantity=missing,
                    source_type="WORK_ORDER_MATERIAL",
                    source_ref=wom["id"],
                    priority=priority,
                )

            touched.add(wo["id"])

        for wo_id in touched:
            self.refresh_work_order_material_state(wo_id, now, tick, rng)

    def refresh_work_order_material_state(
        self,
        wo_id: str,
        now: datetime,
        tick: int,
        rng: random.Random,
    ) -> None:
        wo = self.work_orders[wo_id]
        if wo["status"] not in {"RELEASED", "WAITING_MATERIAL"}:
            return
        remaining = any(
            wom["work_order_id"] == wo_id and wom["status"] != "FULFILLED"
            for wom in self.work_order_materials.values()
        )
        if remaining:
            if wo["status"] == "RELEASED":
                self.transition(
                    "work_order",
                    wo,
                    "WAITING_MATERIAL",
                    now,
                    tick,
                    reason="one or more materials unavailable",
                )
            return

        duration_hours = {
            "CRITICAL": rng.uniform(1.0, 5.0),
            "HIGH": rng.uniform(2.0, 8.0),
            "NORMAL": rng.uniform(3.0, 12.0),
            "LOW": rng.uniform(4.0, 16.0),
        }[wo["priority"]]
        self.transition(
            "work_order",
            wo,
            "IN_PROGRESS",
            now,
            tick,
            reason="all required materials issued",
            extra={
                "started_at": now,
                "complete_after": now + timedelta(hours=duration_hours),
            },
        )

    def reorder_stock(self, now: datetime, tick: int, rng: random.Random) -> None:
        on_order: dict[tuple[str, str], float] = defaultdict(float)
        for poi in self.purchase_order_items.values():
            po = self.purchase_orders[poi["purchase_order_id"]]
            if po["status"] in {"APPROVED", "SENT", "PARTIALLY_RECEIVED"}:
                on_order[(poi["material_id"], po["warehouse_id"])] += max(
                    0.0, poi["quantity"] - poi["received_qty"]
                )

        for (material_id, warehouse_id), balance in list(self.stock.items()):
            material = self.materials[material_id]
            inventory_position = balance["on_hand"] + on_order[(material_id, warehouse_id)]
            if inventory_position >= material["reorder_point"]:
                continue

            source_ref = f"{material_id}:{warehouse_id}"
            active = any(
                pr["source_type"] == "REORDER"
                and pr["source_ref"] == source_ref
                and pr["status"] in {"REQUESTED", "APPROVED", "ORDERED"}
                for pr in self.purchase_requisitions.values()
            )
            if active:
                continue

            quantity = max(
                material["reorder_qty"],
                material["reorder_point"] - inventory_position,
            )
            self.ensure_requisition(
                now=now,
                tick=tick,
                rng=rng,
                warehouse_id=warehouse_id,
                material_id=material_id,
                quantity=quantity,
                source_type="REORDER",
                source_ref=source_ref,
                priority="NORMAL",
            )

    def approve_requisitions(self, now: datetime, tick: int) -> None:
        if not self.is_business_time(now):
            return
        for pr in list(self.purchase_requisitions.values()):
            if pr["status"] == "REQUESTED" and pr["approve_after"] <= now:
                self.transition(
                    "purchase_requisition",
                    pr,
                    "APPROVED",
                    now,
                    tick,
                    reason="approval SLA reached",
                    extra={"approved_at": now},
                )

    def create_purchase_orders(self, now: datetime, tick: int, rng: random.Random) -> None:
        if not self.is_business_time(now):
            return

        for pr in list(self.purchase_requisitions.values()):
            if pr["status"] != "APPROVED":
                continue
            items = [x for x in self.requisition_items.values() if x["requisition_id"] == pr["id"]]
            if not items:
                continue
            # This simulator keeps one item per PR, making supplier selection deterministic.
            pri = items[0]
            material = self.materials[pri["material_id"]]
            supplier = self.suppliers[material["preferred_supplier_id"]]
            po_id = stable_id("purchase_order", pr["id"])

            promised_at = self.next_business_time(now + timedelta(days=supplier["lead_time_days"]))
            if rng.random() <= supplier["delivery_reliability"]:
                actual_delivery = promised_at + timedelta(hours=rng.uniform(-4.0, 5.0))
            else:
                actual_delivery = promised_at + timedelta(days=rng.uniform(1.0, 4.0))

            po = {
                "id": po_id,
                "po_no": f"PO-{po_id[:12].upper()}",
                "requisition_id": pr["id"],
                "supplier_id": supplier["id"],
                "warehouse_id": pr["warehouse_id"],
                "status": "APPROVED",
                "approved_at": now,
                "send_after": now + timedelta(minutes=rng.uniform(10.0, 120.0)),
                "sent_at": None,
                "promised_at": promised_at,
                "next_receipt_at": actual_delivery,
                "received_at": None,
                "closed_at": None,
                "receipt_count": 0,
            }
            self.purchase_orders[po_id] = po
            self.record_initial_state("purchase_order", po, now, tick)

            poi_id = stable_id("purchase_order_item", pri["id"])
            poi = {
                "id": poi_id,
                "purchase_order_id": po_id,
                "requisition_item_id": pri["id"],
                "material_id": pri["material_id"],
                "quantity": pri["quantity"],
                "unit_price_cents": money_with_variance(
                    material["unit_cost_cents"], rng, supplier["price_variability"]
                ),
                "received_qty": 0.0,
            }
            self.purchase_order_items[poi_id] = poi
            self.mark_state("purchase_order_item_state", poi, tick)

            self.transition(
                "purchase_requisition",
                pr,
                "ORDERED",
                now,
                tick,
                reason=f"purchase order {po['po_no']} created",
            )
            self.event(
                now,
                tick,
                "PURCHASE_ORDER_CREATED",
                "purchase_order",
                po_id,
                {"requisition_id": pr["id"], "supplier_id": supplier["id"]},
            )

    def send_purchase_orders(self, now: datetime, tick: int) -> None:
        if not self.is_business_time(now):
            return
        for po in list(self.purchase_orders.values()):
            if po["status"] == "APPROVED" and po["send_after"] <= now:
                self.transition(
                    "purchase_order",
                    po,
                    "SENT",
                    now,
                    tick,
                    reason="purchase order transmitted to supplier",
                    extra={"sent_at": now},
                )

    def receive_purchase_orders(self, now: datetime, tick: int, rng: random.Random) -> None:
        if not self.is_business_time(now):
            return

        for po in list(self.purchase_orders.values()):
            if po["status"] not in {"SENT", "PARTIALLY_RECEIVED"} or po["next_receipt_at"] > now:
                continue
            open_items = [
                item for item in self.purchase_order_items.values()
                if item["purchase_order_id"] == po["id"]
                and item["received_qty"] + 1e-9 < item["quantity"]
            ]
            if not open_items:
                continue

            receipt_index = po["receipt_count"] + 1
            gr_id = stable_id("goods_receipt", po["id"], receipt_index)
            gr = {
                "id": gr_id,
                "receipt_no": f"GR-{gr_id[:12].upper()}",
                "purchase_order_id": po["id"],
                "warehouse_id": po["warehouse_id"],
                "received_at": now,
                "simulation_tick": tick,
            }
            self.append("goods_receipt_store", gr)

            make_partial = (
                po["status"] == "SENT"
                and rng.random() < 0.30
                and any((item["quantity"] - item["received_qty"]) > 1.0 for item in open_items)
            )
            invoice_lines: list[dict[str, Any]] = []

            for item in open_items:
                remaining = item["quantity"] - item["received_qty"]
                if make_partial and remaining > 1.0:
                    qty_received = min(remaining, max(1.0, round(remaining * rng.uniform(0.45, 0.80), 3)))
                else:
                    qty_received = remaining

                gri_id = stable_id("goods_receipt_item", gr_id, item["id"])
                self.append(
                    "goods_receipt_item_store",
                    {
                        "id": gri_id,
                        "goods_receipt_id": gr_id,
                        "purchase_order_item_id": item["id"],
                        "material_id": item["material_id"],
                        "qty_received": float(qty_received),
                        "simulation_tick": tick,
                    },
                )
                item["received_qty"] = round(item["received_qty"] + qty_received, 6)
                self.mark_state("purchase_order_item_state", item, tick)

                balance = self.stock[(item["material_id"], po["warehouse_id"])]
                balance["on_hand"] = round(balance["on_hand"] + qty_received, 6)
                self.mark_state("stock_balance_state", balance, tick)
                self.movement(
                    now=now,
                    tick=tick,
                    material_id=item["material_id"],
                    warehouse_id=po["warehouse_id"],
                    movement_type="RECEIPT",
                    quantity=qty_received,
                    unit_cost_cents=item["unit_price_cents"],
                    reference_type="GOODS_RECEIPT",
                    reference_id=gr_id,
                )
                invoice_lines.append(
                    {
                        "material_id": item["material_id"],
                        "quantity": qty_received,
                        "po_unit_price_cents": item["unit_price_cents"],
                    }
                )

            self.create_fiscal_invoice(now, tick, rng, po, gr_id, invoice_lines)
            po["receipt_count"] = receipt_index

            still_open = any(
                item["purchase_order_id"] == po["id"]
                and item["received_qty"] + 1e-9 < item["quantity"]
                for item in self.purchase_order_items.values()
            )
            if still_open:
                po["next_receipt_at"] = self.next_business_time(now + timedelta(days=rng.uniform(1.0, 3.0)))
                if po["status"] == "SENT":
                    self.transition(
                        "purchase_order",
                        po,
                        "PARTIALLY_RECEIVED",
                        now,
                        tick,
                        reason="supplier delivered only part of the order",
                        extra={"next_receipt_at": po["next_receipt_at"], "receipt_count": receipt_index},
                    )
                else:
                    self.mark_state("purchase_order_state", po, tick)
                    self.event(
                        now,
                        tick,
                        "PURCHASE_ORDER_PARTIAL_RECEIPT",
                        "purchase_order",
                        po["id"],
                        {"receipt_count": receipt_index, "next_receipt_at": po["next_receipt_at"].isoformat()},
                    )
            else:
                self.transition(
                    "purchase_order",
                    po,
                    "RECEIVED",
                    now,
                    tick,
                    reason="all ordered quantities physically received",
                    extra={"received_at": now, "receipt_count": receipt_index},
                )
                pr = self.purchase_requisitions[po["requisition_id"]]
                if pr["status"] == "ORDERED":
                    self.transition(
                        "purchase_requisition",
                        pr,
                        "CLOSED",
                        now,
                        tick,
                        reason="ordered material fully received",
                    )

            self.event(
                now,
                tick,
                "GOODS_RECEIVED",
                "goods_receipt",
                gr_id,
                {"purchase_order_id": po["id"], "partial": still_open},
            )

    def create_fiscal_invoice(
        self,
        now: datetime,
        tick: int,
        rng: random.Random,
        po: dict[str, Any],
        gr_id: str,
        invoice_lines: Iterable[dict[str, Any]],
    ) -> None:
        price_variance_bps = rng.randint(-350, 350) if rng.random() < 0.14 else 0
        tax_issue = rng.random() < 0.04
        subtotal_cents = 0
        tax_total_cents = 0
        total_ipi = 0
        prepared: list[dict[str, Any]] = []

        for line in invoice_lines:
            material = self.materials[line["material_id"]]
            invoice_unit_price = max(
                1,
                round(line["po_unit_price_cents"] * (1.0 + price_variance_bps / 10_000.0)),
            )
            line_base = round(line["quantity"] * invoice_unit_price)
            icms = tax_amount(line_base, material["icms_bps"])
            ipi = tax_amount(line_base, material["ipi_bps"])
            pis = tax_amount(line_base, material["pis_bps"])
            cofins = tax_amount(line_base, material["cofins_bps"])
            subtotal_cents += line_base
            tax_total_cents += icms + ipi + pis + cofins
            total_ipi += ipi
            prepared.append(
                {
                    "material_id": line["material_id"],
                    "quantity": float(line["quantity"]),
                    "unit_price_cents": invoice_unit_price,
                    "icms_cents": icms,
                    "ipi_cents": ipi,
                    "pis_cents": pis,
                    "cofins_cents": cofins,
                }
            )

        inv_id = stable_id("fiscal_invoice", gr_id)
        invoice = {
            "id": inv_id,
            "invoice_no": f"NFE-{inv_id[:12].upper()}",
            "supplier_id": po["supplier_id"],
            "purchase_order_id": po["id"],
            "goods_receipt_id": gr_id,
            "status": "RECEIVED",
            "issued_at": now - timedelta(hours=rng.uniform(1.0, 12.0)),
            "received_at": now,
            "validate_after": self.next_business_time(now + timedelta(hours=rng.uniform(1.0, 6.0))),
            "validation_complete_after": None,
            "blocked_until": None,
            "subtotal_cents": int(subtotal_cents),
            "tax_total_cents": int(tax_total_cents),
            "total_amount_cents": int(subtotal_cents + total_ipi),
            "price_variance_bps": int(price_variance_bps),
            "tax_issue": bool(tax_issue),
        }
        self.invoices[inv_id] = invoice
        self.record_initial_state("fiscal_invoice", invoice, now, tick)

        for line in prepared:
            item_id = stable_id("fiscal_invoice_item", inv_id, line["material_id"])
            self.append(
                "fiscal_invoice_item_store",
                {
                    "id": item_id,
                    "invoice_id": inv_id,
                    **line,
                    "simulation_tick": tick,
                },
            )

        self.event(
            now,
            tick,
            "FISCAL_INVOICE_RECEIVED",
            "fiscal_invoice",
            inv_id,
            {
                "purchase_order_id": po["id"],
                "goods_receipt_id": gr_id,
                "price_variance_bps": price_variance_bps,
                "tax_issue": tax_issue,
            },
        )

    def fiscal_lifecycle(self, now: datetime, tick: int, rng: random.Random) -> None:
        if not self.is_business_time(now):
            return

        # Start validation.
        for invoice in list(self.invoices.values()):
            if invoice["status"] == "RECEIVED" and invoice["validate_after"] <= now:
                self.transition(
                    "fiscal_invoice",
                    invoice,
                    "VALIDATING",
                    now,
                    tick,
                    reason="invoice entered fiscal validation queue",
                    extra={"validation_complete_after": now + timedelta(hours=rng.uniform(1.0, 4.0))},
                )

        # Finish validation in later ticks.
        for invoice in list(self.invoices.values()):
            if (
                invoice["status"] == "VALIDATING"
                and invoice["validation_complete_after"] is not None
                and invoice["validation_complete_after"] <= now
            ):
                price_problem = abs(invoice["price_variance_bps"]) > 50
                tax_problem = invoice["tax_issue"]
                if price_problem or tax_problem:
                    reasons = []
                    if price_problem:
                        reasons.append("price variance above tolerance")
                    if tax_problem:
                        reasons.append("tax validation issue")
                    self.transition(
                        "fiscal_invoice",
                        invoice,
                        "BLOCKED",
                        now,
                        tick,
                        reason="; ".join(reasons),
                        extra={
                            "blocked_until": self.next_business_time(
                                now + timedelta(days=rng.uniform(1.0, 3.0))
                            )
                        },
                    )
                else:
                    self.transition(
                        "fiscal_invoice",
                        invoice,
                        "MATCHED",
                        now,
                        tick,
                        reason="PO, receipt and invoice matched within tolerance",
                    )

        for invoice in list(self.invoices.values()):
            if invoice["status"] == "BLOCKED" and invoice["blocked_until"] <= now:
                self.transition(
                    "fiscal_invoice",
                    invoice,
                    "RELEASED",
                    now,
                    tick,
                    reason="fiscal discrepancy resolved",
                )

        for invoice in list(self.invoices.values()):
            if invoice["status"] == "RELEASED":
                self.transition(
                    "fiscal_invoice",
                    invoice,
                    "MATCHED",
                    now,
                    tick,
                    reason="released invoice revalidated",
                )

    def finance_lifecycle(self, now: datetime, tick: int) -> None:
        if not self.is_business_time(now):
            return

        invoices_with_ap = {ap["invoice_id"] for ap in self.accounts_payable.values()}
        for invoice in list(self.invoices.values()):
            if invoice["status"] != "MATCHED" or invoice["id"] in invoices_with_ap:
                continue
            supplier = self.suppliers[invoice["supplier_id"]]
            due_at = self.next_business_time(now + timedelta(days=supplier["payment_term_days"]))
            ap_id = stable_id("accounts_payable", invoice["id"])
            ap = {
                "id": ap_id,
                "invoice_id": invoice["id"],
                "supplier_id": invoice["supplier_id"],
                "status": "OPEN",
                "amount_cents": invoice["total_amount_cents"],
                "posted_at": now,
                "due_at": due_at,
                "scheduled_at": None,
                "payment_scheduled_for": None,
                "paid_at": None,
            }
            self.accounts_payable[ap_id] = ap
            self.record_initial_state("accounts_payable", ap, now, tick)
            self.transition(
                "fiscal_invoice",
                invoice,
                "POSTED",
                now,
                tick,
                reason="invoice posted to accounts payable",
            )
            self.event(
                now,
                tick,
                "ACCOUNTS_PAYABLE_CREATED",
                "accounts_payable",
                ap_id,
                {"invoice_id": invoice["id"], "amount_cents": ap["amount_cents"]},
            )

        schedule_horizon = now + timedelta(days=3)
        for ap in list(self.accounts_payable.values()):
            if ap["status"] == "OPEN" and ap["due_at"] <= schedule_horizon:
                payment_date = self.next_business_time(ap["due_at"])
                self.transition(
                    "accounts_payable",
                    ap,
                    "SCHEDULED",
                    now,
                    tick,
                    reason="invoice entered payment run",
                    extra={"scheduled_at": now, "payment_scheduled_for": payment_date},
                )

        for ap in list(self.accounts_payable.values()):
            if (
                ap["status"] == "SCHEDULED"
                and ap["payment_scheduled_for"] is not None
                and ap["payment_scheduled_for"] <= now
            ):
                payment_id = stable_id("payment", ap["id"])
                self.append(
                    "payment_store",
                    {
                        "id": payment_id,
                        "payment_no": f"PAY-{payment_id[:12].upper()}",
                        "accounts_payable_id": ap["id"],
                        "amount_cents": ap["amount_cents"],
                        "paid_at": now,
                        "simulation_tick": tick,
                    },
                )
                self.transition(
                    "accounts_payable",
                    ap,
                    "PAID",
                    now,
                    tick,
                    reason="payment executed",
                    extra={"paid_at": now},
                )
                self.event(
                    now,
                    tick,
                    "PAYMENT_EXECUTED",
                    "payment",
                    payment_id,
                    {"accounts_payable_id": ap["id"], "amount_cents": ap["amount_cents"]},
                )

    def close_purchase_orders(self, now: datetime, tick: int) -> None:
        if not self.is_business_time(now):
            return
        for po in list(self.purchase_orders.values()):
            if po["status"] != "RECEIVED":
                continue
            invoices = [inv for inv in self.invoices.values() if inv["purchase_order_id"] == po["id"]]
            if invoices and all(inv["status"] == "POSTED" for inv in invoices):
                self.transition(
                    "purchase_order",
                    po,
                    "CLOSED",
                    now,
                    tick,
                    reason="all receipts invoiced and posted",
                    extra={"closed_at": now},
                )

    def complete_work_orders(self, now: datetime, tick: int, rng: random.Random) -> None:
        for wo in list(self.work_orders.values()):
            if wo["status"] == "IN_PROGRESS" and wo["complete_after"] <= now:
                self.transition(
                    "work_order",
                    wo,
                    "COMPLETED",
                    now,
                    tick,
                    reason="maintenance execution finished",
                    extra={
                        "completed_at": now,
                        "close_after": now + timedelta(hours=rng.uniform(1.0, 8.0)),
                    },
                )

        if not self.is_business_time(now):
            return
        for wo in list(self.work_orders.values()):
            if wo["status"] == "COMPLETED" and wo["close_after"] <= now:
                self.transition(
                    "work_order",
                    wo,
                    "CLOSED",
                    now,
                    tick,
                    reason="maintenance order administratively closed",
                    extra={"closed_at": now},
                )

    def cycle_counts(self, now: datetime, tick: int, rng: random.Random) -> None:
        if not self.is_business_time(now):
            return
        count = poisson(rng, self.config.cycle_counts_per_day * self.config.step_minutes / 1440.0)
        keys = list(self.stock.keys())
        for _ in range(count):
            key = rng.choice(keys)
            balance = self.stock[key]
            delta = 0.0 if rng.random() < 0.78 else float(rng.choice([-2, -1, 1, 1, 2]))
            if balance["on_hand"] + delta < 0:
                delta = -balance["on_hand"]
            balance["on_hand"] = round(balance["on_hand"] + delta, 6)
            balance["last_counted_at"] = now
            self.mark_state("stock_balance_state", balance, tick)
            self.event(
                now,
                tick,
                "CYCLE_COUNT_COMPLETED",
                "material",
                balance["material_id"],
                {"warehouse_id": balance["warehouse_id"], "adjustment": delta},
            )
            if abs(delta) > 1e-9:
                self.movement(
                    now=now,
                    tick=tick,
                    material_id=balance["material_id"],
                    warehouse_id=balance["warehouse_id"],
                    movement_type="CYCLE_COUNT_ADJUSTMENT",
                    quantity=delta,
                    unit_cost_cents=self.materials[balance["material_id"]]["unit_cost_cents"],
                    reference_type="CYCLE_COUNT",
                    reference_id=stable_id("cycle_count", tick, balance["material_id"], balance["warehouse_id"]),
                )

    def run_tick(self, previous_time: datetime, tick: int) -> datetime:
        now = previous_time + timedelta(minutes=self.config.step_minutes)
        rng = random.Random(self.config.seed + tick * 1_000_003)

        self.create_work_orders(now, tick, rng)
        self.release_work_orders(now, tick)
        self.issue_pending_materials(now, tick, rng)
        self.reorder_stock(now, tick, rng)
        self.approve_requisitions(now, tick)
        self.create_purchase_orders(now, tick, rng)
        self.send_purchase_orders(now, tick)
        self.receive_purchase_orders(now, tick, rng)

        # A receipt can unblock a maintenance order in the same simulated tick.
        self.issue_pending_materials(now, tick, rng)

        self.fiscal_lifecycle(now, tick, rng)
        self.finance_lifecycle(now, tick)
        self.close_purchase_orders(now, tick)
        self.complete_work_orders(now, tick, rng)
        self.cycle_counts(now, tick, rng)

        return now


# ---------------------------------------------------------------------------
# Databricks notebook parameters and orchestration
# ---------------------------------------------------------------------------

WIDGET_DEFAULTS: dict[str, str] = {
    "catalog": "main",
    "schema": "mro_sim",
    "start": "2026-01-01T08:00:00-03:00",
    "step_minutes": "60",
    "ticks_per_run": "1",
    "commit_every_ticks": "1",
    "seed": "42",
    "work_orders_per_day": "8",
    "cycle_counts_per_day": "3",
    "business_timezone": "America/Sao_Paulo",
    "bootstrap_only": "false",
    "reset": "false",
}


def _dbutils():
    """Return the Databricks ``dbutils`` object with a useful local error."""
    try:
        return dbutils  # type: ignore[name-defined]
    except NameError as exc:
        raise RuntimeError(
            "This entry point expects a Databricks notebook runtime because "
            "configuration is read from dbutils.widgets. Import the module and "
            "call run(Config(...)) directly for local/unit testing."
        ) from exc


def ensure_notebook_widgets() -> None:
    """Create missing widgets without overwriting values supplied by a Job."""
    widgets = _dbutils().widgets
    for name, default in WIDGET_DEFAULTS.items():
        try:
            widgets.get(name)
        except Exception:
            widgets.text(name, default)


def widget_value(name: str) -> str:
    if name not in WIDGET_DEFAULTS:
        raise KeyError(f"Unknown notebook parameter: {name}")
    return _dbutils().widgets.get(name).strip()


def parse_bool(value: str, *, parameter: str) -> bool:
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    raise ValueError(
        f"Notebook parameter {parameter!r} must be true/false; got {value!r}"
    )


def load_notebook_config() -> Config:
    """Build and validate simulator configuration from Databricks widgets."""
    ensure_notebook_widgets()

    catalog = widget_value("catalog")
    schema = widget_value("schema")
    start = widget_value("start")
    business_timezone = widget_value("business_timezone")

    validate_identifier(catalog, "catalog")
    validate_identifier(schema, "schema")
    ZoneInfo(business_timezone)  # validate early

    try:
        step_minutes = int(widget_value("step_minutes"))
        ticks_per_run = int(widget_value("ticks_per_run"))
        commit_every_ticks = int(widget_value("commit_every_ticks"))
        seed = int(widget_value("seed"))
        work_orders_per_day = float(widget_value("work_orders_per_day"))
        cycle_counts_per_day = float(widget_value("cycle_counts_per_day"))
    except ValueError as exc:
        raise ValueError(
            "Numeric notebook parameters contain an invalid value. "
            "Check step_minutes, ticks_per_run, commit_every_ticks, seed, "
            "work_orders_per_day, and cycle_counts_per_day."
        ) from exc

    if step_minutes <= 0:
        raise ValueError("step_minutes must be > 0")
    if ticks_per_run < 0:
        raise ValueError("ticks_per_run must be >= 0")
    if commit_every_ticks <= 0:
        raise ValueError("commit_every_ticks must be > 0")
    if work_orders_per_day < 0:
        raise ValueError("work_orders_per_day must be >= 0")
    if cycle_counts_per_day < 0:
        raise ValueError("cycle_counts_per_day must be >= 0")

    return Config(
        catalog=catalog,
        schema=schema,
        start=parse_timestamp(start),
        step_minutes=step_minutes,
        ticks_per_run=ticks_per_run,
        commit_every_ticks=commit_every_ticks,
        seed=seed,
        work_orders_per_day=work_orders_per_day,
        cycle_counts_per_day=cycle_counts_per_day,
        business_timezone=business_timezone,
        reset=parse_bool(widget_value("reset"), parameter="reset"),
        bootstrap_only=parse_bool(
            widget_value("bootstrap_only"),
            parameter="bootstrap_only",
        ),
    )


def reset_schema(spark: SparkSession, config: Config) -> None:
    spark.sql(f"DROP SCHEMA IF EXISTS {config.namespace} CASCADE")


def run(config: Config) -> None:
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    if config.reset:
        reset_schema(spark, config)

    store = DeltaStore(spark, config)
    store.bootstrap_schema()
    engine = SimulationEngine(store, config)
    engine.seed_master_data()
    engine.bootstrap_state_if_needed()
    store.create_views()

    if config.bootstrap_only or config.ticks_per_run == 0:
        state = store.load_state()
        print(
            json.dumps(
                {
                    "status": "BOOTSTRAPPED",
                    "catalog": config.catalog,
                    "schema": config.schema,
                    "simulated_at": state["simulated_at"].isoformat(),
                    "committed_tick": state["committed_tick"],
                },
                indent=2,
            )
        )
        return

    simulated_at, committed_tick = engine.load()
    run_id = str(uuid.uuid4())
    run_row = {
        "run_id": run_id,
        "started_at": utc_now(),
        "finished_at": None,
        "from_tick": committed_tick,
        "to_tick": None,
        "status": "RUNNING",
        "error": None,
    }
    store.replace_run(run_row)

    target_tick = committed_tick + config.ticks_per_run
    print(
        f"MRO simulation run={run_id} schema={config.catalog}.{config.schema} "
        f"ticks={committed_tick + 1}..{target_tick}"
    )

    try:
        ticks_since_commit = 0
        for tick in range(committed_tick + 1, target_tick + 1):
            simulated_at = engine.run_tick(simulated_at, tick)
            ticks_since_commit += 1
            print(
                f"tick={tick} simulated_at={simulated_at.isoformat()} "
                f"pending_commit={ticks_since_commit}"
            )

            if ticks_since_commit >= config.commit_every_ticks or tick == target_tick:
                engine.flush(simulated_at, tick)
                ticks_since_commit = 0
                print(f"committed_tick={tick}")

        run_row.update(
            {
                "finished_at": utc_now(),
                "to_tick": target_tick,
                "status": "SUCCEEDED",
                "error": None,
            }
        )
        store.replace_run(run_row)

        print(
            json.dumps(
                {
                    "status": "SUCCEEDED",
                    "run_id": run_id,
                    "catalog": config.catalog,
                    "schema": config.schema,
                    "from_tick": committed_tick,
                    "to_tick": target_tick,
                    "simulated_at": simulated_at.isoformat(),
                },
                indent=2,
            )
        )
    except Exception as exc:
        run_row.update(
            {
                "finished_at": utc_now(),
                "to_tick": store.load_state()["committed_tick"],
                "status": "FAILED",
                "error": "".join(
                    traceback.format_exception(type(exc), exc, exc.__traceback__)
                )[-16000:],
            }
        )
        store.replace_run(run_row)
        raise


def main() -> None:
    """Databricks Notebook task entry point."""
    config = load_notebook_config()

    print(
        json.dumps(
            {
                "catalog": config.catalog,
                "schema": config.schema,
                "start": config.start.isoformat(),
                "step_minutes": config.step_minutes,
                "ticks_per_run": config.ticks_per_run,
                "commit_every_ticks": config.commit_every_ticks,
                "seed": config.seed,
                "work_orders_per_day": config.work_orders_per_day,
                "cycle_counts_per_day": config.cycle_counts_per_day,
                "business_timezone": config.business_timezone,
                "bootstrap_only": config.bootstrap_only,
                "reset": config.reset,
            },
            indent=2,
        )
    )

    run(config)


if __name__ == "__main__":
    main()
