"""
Central configuration for datasets, KPI thresholds, and output defaults.

This file sits in the configuration layer of the project. It defines where data
and outputs live, how each dataset should be validated, and what target ranges
drive KPI status evaluation across the pipeline.
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional


@dataclass(frozen=True)
class PathsConfig:
    # ================================
    # Class: PathsConfig
    # Purpose: Stores the key filesystem paths used by the project.
    # Inputs:
    #   - None at runtime; defaults are derived from the package location
    # Output:
    #   - Immutable config object with project, data, output, and template paths
    # ================================
    project_root: Path = Path(__file__).resolve().parents[1]
    data_dir: Path = field(default_factory=lambda: Path(__file__).resolve().parents[1] / "data")
    output_dir: Path = field(default_factory=lambda: Path(__file__).resolve().parents[1] / "output")
    template_path: Path = field(default_factory=lambda: Path(__file__).resolve().parents[1] / "templates" / "one_pager_example.xlsx")


@dataclass(frozen=True)
class KPIThreshold:
    # ================================
    # Class: KPIThreshold
    # Purpose: Defines the business rule used to assign KPI status.
    # Inputs:
    #   - target, direction, amber, and optional band bounds
    # Output:
    #   - Immutable threshold object consumed by KPI evaluation logic
    # ================================
    target: Optional[float]
    direction: str  # ge, le, band, info
    amber: Optional[float] = None
    good_low: Optional[float] = None
    good_high: Optional[float] = None
    amber_low: Optional[float] = None
    amber_high: Optional[float] = None
    unit: str = "number"


# Dataset-level schema and typing rules used during ingestion and validation.
DATASET_CONFIG = {
    "inbound_parts": {
        "file": "inbound_parts.csv",
        "date_columns": ["expected_date", "received_date"],
        "required_columns": [
            "po_number", "supplier_id", "supplier_name", "part_number", "expected_date", "received_date",
            "qty_ordered", "qty_received", "inbound_lead_time_days", "discrepancy_qty"
        ],
        "numeric_columns": ["qty_ordered", "qty_received", "inbound_lead_time_days", "discrepancy_qty"],
        "warehouse_aware": False,
        "event_date": "received_date",
    },
    "outbound_parts": {
        "file": "outbound_parts.csv",
        "date_columns": ["order_date", "promise_date", "shipped_date"],
        "required_columns": [
            "order_number", "customer_id", "customer_name", "part_number", "order_date", "promise_date", "shipped_date",
            "qty_ordered", "qty_shipped", "backorder_qty", "otif_flag", "fill_rate"
        ],
        "numeric_columns": ["qty_ordered", "qty_shipped", "backorder_qty", "otif_flag", "fill_rate"],
        "warehouse_aware": False,
        "event_date": "shipped_date",
    },
    "inventory_snapshot": {
        "file": "inventory_snapshot.csv",
        "date_columns": ["snapshot_date"],
        "required_columns": [
            "snapshot_date", "warehouse_id", "warehouse_name", "location", "part_number", "sku_family", "on_hand_qty",
            "available_qty", "safety_stock", "reorder_point", "days_of_supply", "stockout_flag", "age_days"
        ],
        "numeric_columns": ["on_hand_qty", "available_qty", "safety_stock", "reorder_point", "days_of_supply", "stockout_flag", "age_days"],
        "warehouse_aware": True,
        "event_date": "snapshot_date",
    },
    "warehouse_productivity": {
        "file": "warehouse_productivity.csv",
        "date_columns": ["date"],
        "required_columns": [
            "date", "warehouse_id", "shift", "lines_picked", "lines_packed", "orders_processed", "labor_hours",
            "picks_per_hour", "touches_per_order", "equipment_utilization_pct", "sla_adherence_pct"
        ],
        "numeric_columns": ["lines_picked", "lines_packed", "orders_processed", "labor_hours", "picks_per_hour", "touches_per_order", "equipment_utilization_pct", "sla_adherence_pct"],
        "warehouse_aware": True,
        "event_date": "date",
    },
    "employee_productivity": {
        "file": "employee_productivity.csv",
        "date_columns": ["date"],
        "required_columns": [
            "date", "employee_id", "role", "warehouse_id", "shift", "tasks_completed", "picks", "hours_worked",
            "picks_per_hour", "errors", "rework", "overtime_hours"
        ],
        "numeric_columns": ["tasks_completed", "picks", "hours_worked", "picks_per_hour", "errors", "rework", "overtime_hours"],
        "warehouse_aware": True,
        "event_date": "date",
    },
}


# Business thresholds that convert KPI values into green/amber/red/info status.
THRESHOLDS: Dict[str, KPIThreshold] = {
    "Average Inbound Lead Time": KPIThreshold(target=5.0, direction="le", amber=8.0, unit="days"),
    "Receipts On-Time %": KPIThreshold(target=0.95, direction="ge", amber=0.90, unit="pct"),
    "Quantity Discrepancy %": KPIThreshold(target=0.02, direction="le", amber=0.05, unit="pct"),
    "Inbound Volume": KPIThreshold(target=None, direction="info", unit="qty"),
    "Late Receipt Count": KPIThreshold(target=None, direction="info", unit="count"),
    "Top 5 Delaying Suppliers": KPIThreshold(target=None, direction="info", unit="text"),
    "Fill Rate %": KPIThreshold(target=0.95, direction="ge", amber=0.90, unit="pct"),
    "OTIF %": KPIThreshold(target=0.92, direction="ge", amber=0.85, unit="pct"),
    "Backorder Rate %": KPIThreshold(target=0.03, direction="le", amber=0.07, unit="pct"),
    "Outbound Volume": KPIThreshold(target=None, direction="info", unit="qty"),
    "Late Shipment Count": KPIThreshold(target=None, direction="info", unit="count"),
    "Top 10 SKUs by Backorder": KPIThreshold(target=None, direction="info", unit="text"),
    "Days of Supply": KPIThreshold(target=30.0, direction="band", good_low=20.0, good_high=45.0, amber_low=10.0, amber_high=60.0, unit="days"),
    "Stockout Exposure %": KPIThreshold(target=0.02, direction="le", amber=0.05, unit="pct"),
    "Safety Stock Coverage %": KPIThreshold(target=0.95, direction="ge", amber=0.90, unit="pct"),
    "Aged Inventory % (>180d)": KPIThreshold(target=0.15, direction="le", amber=0.25, unit="pct"),
    "Average Inventory Age": KPIThreshold(target=None, direction="info", unit="days"),
    "Inventory Turns": KPIThreshold(target=None, direction="info", unit="rate"),
    "Aged Inventory Value >180d": KPIThreshold(target=None, direction="info", unit="qty"),
    "Lines Picked per Labor-Hour": KPIThreshold(target=14.0, direction="ge", amber=12.0, unit="rate"),
    "Orders Processed per Labor-Hour": KPIThreshold(target=1.2, direction="ge", amber=1.0, unit="rate"),
    "Orders per Day": KPIThreshold(target=None, direction="info", unit="rate"),
    "SLA Adherence %": KPIThreshold(target=0.95, direction="ge", amber=0.90, unit="pct"),
    "Equipment Utilization %": KPIThreshold(target=0.80, direction="band", good_low=0.75, good_high=0.90, amber_low=0.65, amber_high=0.95, unit="pct"),
    "Touches per Order": KPIThreshold(target=3.5, direction="le", amber=4.0, unit="rate"),
    "Picks per Person per Hour": KPIThreshold(target=14.0, direction="ge", amber=12.0, unit="rate"),
    "Error Rate %": KPIThreshold(target=0.01, direction="le", amber=0.02, unit="pct"),
    "Rework Rate %": KPIThreshold(target=0.015, direction="le", amber=0.03, unit="pct"),
    "Overtime %": KPIThreshold(target=0.08, direction="le", amber=0.12, unit="pct"),
    "Average Tasks per Employee": KPIThreshold(target=None, direction="info", unit="count"),
}


# Order used when selecting the executive summary cards for top-level output.
SUMMARY_CARD_ORDER = [
    "Fill Rate %",
    "OTIF %",
    "Days of Supply",
    "Lines Picked per Labor-Hour",
    "Error Rate %",
]


# Assumptions surfaced in the audit section so consumers can trace KPI meaning.
ASSUMPTIONS: List[str] = [
    "Default reporting period uses the latest full month available across core operational event dates.",
    "Percent KPIs are computed from source quantities or flags and rendered as percentages in outputs.",
    "Inventory days of supply uses the provided operational field rather than a reconstructed demand model.",
    "Narrative insights default to deterministic business rules, with optional OpenAI wording support.",
]


CALCULATION_VERSION = "v1.0.0"


# Configuration for the optional LLM narrative layer.
LLM_SUMMARY_CONFIG = {
    "enabled_by_default": True,
    "provider": "openai",
    "model": "gpt-4.1-mini",
    "purpose": "Optional narrative refinement layer. KPI calculations remain deterministic.",
}


# Drill-down dimensions made available per section. Sections can request
# dimensions that are not present in a given dataset; downstream logic will skip
# unavailable dimensions gracefully while preserving the configured order.
DRILLDOWN_CONFIG: Dict[str, Dict[str, object]] = {
    "Inbound": {
        "source_dataset": "inbound_parts",
        "dimension_order": [
            "by_warehouse",
            "by_supplier",
            "by_sku_family",
            "by_part_number",
            "by_date",
        ],
        "dimensions": {
            "by_warehouse": ["warehouse_id"],
            "by_supplier": ["supplier_name"],
            "by_sku_family": ["sku_family"],
            "by_part_number": ["part_number"],
            "by_date": ["receipt_date"],
        },
    },
    "Outbound": {
        "source_dataset": "outbound_parts",
        "dimension_order": [
            "by_warehouse",
            "by_customer",
            "by_sku_family",
            "by_part_number",
            "by_date",
        ],
        "dimensions": {
            "by_warehouse": ["warehouse_id"],
            "by_customer": ["customer_name"],
            "by_sku_family": ["sku_family"],
            "by_part_number": ["part_number"],
            "by_date": ["ship_date"],
        },
    },
    "Inventory": {
        "source_dataset": "inventory_snapshot",
        "dimension_order": [
            "by_warehouse",
            "by_sku_family",
            "by_part_number",
            "by_date",
        ],
        "dimensions": {
            "by_warehouse": ["warehouse_id"],
            "by_sku_family": ["sku_family"],
            "by_part_number": ["part_number"],
            "by_date": ["snapshot_date"],
        },
    },
    "Warehouse Productivity": {
        "source_dataset": "warehouse_productivity",
        "dimension_order": [
            "by_warehouse",
            "by_date",
            "by_shift",
        ],
        "dimensions": {
            "by_warehouse": ["warehouse_id"],
            "by_date": ["operation_date"],
            "by_shift": ["shift"],
        },
    },
    "Employee Productivity": {
        "source_dataset": "employee_productivity",
        "dimension_order": [
            "by_warehouse",
            "by_employee",
            "by_date",
            "by_shift",
        ],
        "dimensions": {
            "by_warehouse": ["warehouse_id"],
            "by_employee": ["employee_id"],
            "by_date": ["work_date"],
            "by_shift": ["shift"],
        },
    },
}


DRILLDOWN_DIMENSION_LABELS: Dict[str, str] = {
    "warehouse_id": "Warehouse",
    "supplier_name": "Supplier",
    "customer_name": "Customer",
    "sku_family": "SKU Family",
    "part_number": "Part Number",
    "employee_id": "Employee",
    "receipt_date": "Receipt Date",
    "ship_date": "Shipped Date",
    "snapshot_date": "Snapshot Date",
    "operation_date": "Date",
    "work_date": "Date",
    "shift": "Shift",
}


RAW_DETAIL_COLUMNS: Dict[str, List[str]] = {
    "Inbound": [
        "po_number",
        "supplier_name",
        "part_number",
        "expected_date",
        "received_date",
        "receipt_on_time_flag",
        "inbound_lead_time_days",
        "qty_ordered",
        "qty_received",
        "discrepancy_qty",
    ],
    "Outbound": [
        "order_number",
        "customer_name",
        "part_number",
        "order_date",
        "promise_date",
        "shipped_date",
        "late_shipment_flag",
        "qty_ordered",
        "qty_shipped",
        "backorder_qty",
        "otif_flag",
        "fill_rate",
    ],
    "Inventory": [
        "snapshot_date",
        "warehouse_id",
        "warehouse_name",
        "part_number",
        "sku_family",
        "on_hand_qty",
        "available_qty",
        "safety_stock",
        "below_safety_stock_flag",
        "days_of_supply",
        "stockout_flag",
        "age_days",
    ],
    "Warehouse Productivity": [
        "date",
        "warehouse_id",
        "shift",
        "lines_picked",
        "lines_packed",
        "orders_processed",
        "labor_hours",
        "picks_per_hour",
        "touches_per_order",
        "equipment_utilization_pct",
        "sla_adherence_pct",
    ],
    "Employee Productivity": [
        "date",
        "employee_id",
        "role",
        "warehouse_id",
        "shift",
        "tasks_completed",
        "picks",
        "hours_worked",
        "picks_per_hour",
        "errors",
        "rework",
        "overtime_hours",
    ],
}
