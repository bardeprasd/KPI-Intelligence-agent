"""
Deterministic KPI calculation logic for all operational domains.

This file sits in the KPI computation layer of the project. It filters datasets
to the requested scope, calculates supply-chain and warehouse metrics, evaluates
status against configured thresholds, and packages results into a common schema.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Sequence, Tuple

import pandas as pd

from .config import DRILLDOWN_CONFIG, DRILLDOWN_DIMENSION_LABELS, KPIThreshold, RAW_DETAIL_COLUMNS, THRESHOLDS


MetricCalculator = Callable[[pd.DataFrame], float]


@dataclass(frozen=True)
class MetricSpec:
    # Declarative KPI spec so the same metric can be reused in top-level
    # sections, grouped drill-downs, and traceability output.
    name: str
    formula: str
    source_table: str
    compute: MetricCalculator


def safe_divide(numerator: float, denominator: float) -> float:
    # KPI math should never fail on empty or zero-denominator slices.
    return float(numerator / denominator) if denominator not in (0, 0.0) else 0.0


def filter_period(df: pd.DataFrame, date_col: str, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    return df[(df[date_col] >= start) & (df[date_col] <= end)].copy()


def filter_warehouses(df: pd.DataFrame, warehouses: Optional[Sequence[str]]) -> pd.DataFrame:
    if not warehouses or "warehouse_id" not in df.columns:
        return df
    return df[df["warehouse_id"].isin(warehouses)].copy()


def filter_sku_families(
    df: pd.DataFrame,
    sku_families: Optional[Sequence[str]],
    allowed_parts: Optional[Sequence[str]] = None,
) -> pd.DataFrame:
    if not sku_families:
        return df
    if "sku_family" in df.columns:
        return df[df["sku_family"].isin(sku_families)].copy()
    if allowed_parts is not None and "part_number" in df.columns:
        return df[df["part_number"].isin(allowed_parts)].copy()
    return df


def evaluate_status(value: float, threshold: KPIThreshold) -> str:
    # Convert raw KPI values into leadership-friendly traffic-light status.
    if threshold.direction == "info" or threshold.target is None:
        return "info"
    if threshold.direction == "ge":
        if value >= threshold.target:
            return "green"
        if threshold.amber is not None and value >= threshold.amber:
            return "amber"
        return "red"
    if threshold.direction == "le":
        if value <= threshold.target:
            return "green"
        if threshold.amber is not None and value <= threshold.amber:
            return "amber"
        return "red"
    if threshold.direction == "band":
        if threshold.good_low <= value <= threshold.good_high:
            return "green"
        if threshold.amber_low <= value <= threshold.amber_high:
            return "amber"
        return "red"
    return "info"


def format_value(value: float, unit: str) -> str:
    if unit == "pct":
        return f"{value * 100:.1f}%"
    if unit == "days":
        return f"{value:.1f} days"
    if unit in {"qty", "count"}:
        return f"{value:,.0f}"
    if unit == "text":
        return str(value)
    return f"{value:.2f}"


def format_target(threshold: KPIThreshold) -> Optional[str]:
    if threshold.direction == "info" or threshold.target is None:
        return None
    if threshold.direction == "ge":
        if threshold.unit == "pct":
            return f">= {threshold.target * 100:.1f}%"
        return f">= {threshold.target:.2f}"
    if threshold.direction == "le":
        if threshold.unit == "pct":
            return f"<= {threshold.target * 100:.1f}%"
        return f"<= {threshold.target:.2f}"
    if threshold.direction == "band":
        if threshold.unit == "pct":
            return f"{threshold.good_low * 100:.1f}% to {threshold.good_high * 100:.1f}%"
        return f"{threshold.good_low:.1f} to {threshold.good_high:.1f}"
    return None


def _top_labels(series: pd.Series, count: int = 2) -> List[str]:
    labels: List[str] = []
    for item in series.index.tolist()[:count]:
        text = str(item).strip()
        if text and text.lower() != "nan":
            labels.append(text)
    return labels


def _generic_comment(value: float, threshold: KPIThreshold) -> str:
    # Fallback narrative used when the section cannot name a concrete business
    # driver such as a supplier, SKU, warehouse, employee, or shift.
    status = evaluate_status(value, threshold)
    if threshold.direction == "info" or threshold.target is None:
        return "Informational KPI"
    if threshold.direction == "ge":
        if status == "green":
            gap = value - threshold.target
            return "Slightly above target" if threshold.target and gap <= threshold.target * 0.05 else "Above target"
        if status == "amber":
            return "Slightly below target"
        return "Materially below target"
    if threshold.direction == "le":
        if status == "green":
            gap = threshold.target - value
            return "Within tolerance" if threshold.target and gap <= max(threshold.target * 0.25, 0.01) else "Healthy"
        if status == "amber":
            return "Slightly above tolerance"
        return "Above tolerance"
    if threshold.direction == "band":
        if status == "green":
            return "Within tolerance"
        midpoint = ((threshold.good_low or 0.0) + (threshold.good_high or 0.0)) / 2
        return "Below target range" if value < midpoint else "Above target range"
    return "Current-period view"


def build_kpi(
    name: str,
    value: float,
    formula: str,
    source_table: str,
    grain: str,
    note: str = "",
    display_override: Optional[str] = None,
) -> Dict:
    # Canonical KPI object shared by summaries, exports, drill-downs, and chat.
    threshold = THRESHOLDS[name]
    return {
        "name": name,
        "value": round(float(value), 4),
        "display_value": display_override if display_override is not None else format_value(float(value), threshold.unit),
        "unit": threshold.unit,
        "status": evaluate_status(float(value), threshold),
        "target": threshold.target,
        "target_display": format_target(threshold),
        "formula": formula,
        "source_table": source_table,
        "grain": grain,
        "note": note,
    }


def _inventory_part_mapping(inventory: pd.DataFrame, column: str) -> Dict[str, str]:
    # Inventory is the only dataset that reliably carries SKU-family style
    # attributes, so inbound/outbound rows borrow that mapping by part number.
    if "part_number" not in inventory.columns or column not in inventory.columns:
        return {}
    mapping_source = inventory[["part_number", column]].dropna().drop_duplicates()
    if mapping_source.empty:
        return {}
    mapping_source = mapping_source.sort_values(["part_number", column]).drop_duplicates(subset=["part_number"], keep="first")
    return dict(zip(mapping_source["part_number"].astype(str), mapping_source[column].astype(str)))


def _enrich_inbound_outbound_dimensions(df: pd.DataFrame, inventory: pd.DataFrame, *, date_alias: str) -> pd.DataFrame:
    enriched = df.copy()
    sku_family_map = _inventory_part_mapping(inventory, "sku_family")
    if "sku_family" not in enriched.columns and "part_number" in enriched.columns and sku_family_map:
        enriched["sku_family"] = enriched["part_number"].astype(str).map(sku_family_map)
    if date_alias not in enriched.columns:
        if date_alias == "receipt_date" and "received_date" in enriched.columns:
            enriched[date_alias] = enriched["received_date"]
        elif date_alias == "ship_date" and "shipped_date" in enriched.columns:
            enriched[date_alias] = enriched["shipped_date"]
    return enriched


def _prepare_inventory_drilldown_df(df: pd.DataFrame) -> pd.DataFrame:
    prepared = df.copy()
    if "snapshot_date" in prepared.columns:
        prepared["snapshot_date"] = pd.to_datetime(prepared["snapshot_date"], errors="coerce")
    return prepared


def _prepare_warehouse_productivity_drilldown_df(df: pd.DataFrame) -> pd.DataFrame:
    prepared = df.copy()
    if "date" in prepared.columns:
        prepared["operation_date"] = pd.to_datetime(prepared["date"], errors="coerce")
    return prepared


def _prepare_employee_productivity_drilldown_df(df: pd.DataFrame) -> pd.DataFrame:
    prepared = df.copy()
    if "date" in prepared.columns:
        prepared["work_date"] = pd.to_datetime(prepared["date"], errors="coerce")
    return prepared


def _group_key_sortable(value: object) -> object:
    if pd.isna(value):
        return ""
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return str(value)


def _serialize_dimension_value(value: object) -> object:
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.date().isoformat()
    return value.item() if hasattr(value, "item") else value


def _serialize_table_value(value: object) -> object:
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.date().isoformat()
    return value.item() if hasattr(value, "item") else value


def _serialize_filters(
    *,
    start: pd.Timestamp,
    end: pd.Timestamp,
    warehouses: Optional[Sequence[str]],
    sku_families: Optional[Sequence[str]],
) -> Dict[str, object]:
    return {
        "reporting_period": {
            "start_date": start.date().isoformat(),
            "end_date": end.date().isoformat(),
        },
        "warehouse_filter": list(warehouses) if warehouses else None,
        "sku_family_filter": list(sku_families) if sku_families else None,
    }


def _metric_snapshot(spec: MetricSpec, frame: pd.DataFrame, grain: str) -> Dict:
    # Reuse the same MetricSpec logic when computing grouped drill-down rows.
    value = float(spec.compute(frame))
    return build_kpi(
        name=spec.name,
        value=value,
        formula=spec.formula,
        source_table=spec.source_table,
        grain=grain,
    )


def build_raw_detail(
    df: pd.DataFrame,
    *,
    section_name: str,
    source_dataset: str,
    applied_filters: Dict[str, object],
    sort_by: Sequence[str],
    logic_note: str,
) -> Dict:
    # Preserve the exact scoped records behind a section so downstream HTML and
    # audit consumers can trace each KPI back to raw rows.
    columns = [column for column in RAW_DETAIL_COLUMNS[section_name] if column in df.columns]
    scoped = df.copy()
    available_sort = [column for column in sort_by if column in scoped.columns]
    if available_sort:
        scoped = scoped.sort_values(available_sort, ascending=[False] * len(available_sort), na_position="last")

    rows: List[Dict] = []
    for _, row in scoped[columns].iterrows():
        rows.append({column: _serialize_table_value(row[column]) for column in columns})

    return {
        "source_dataset": source_dataset,
        "applied_filters": applied_filters,
        "columns": columns,
        "row_count": len(rows),
        "logic_note": logic_note,
        "rows": rows,
    }


def _build_drilldown_unavailable(
    *,
    drilldown_id: str,
    group_by: Sequence[str],
    source_dataset: str,
    applied_filters: Dict[str, object],
    reason: str,
) -> Dict:
    return {
        "id": drilldown_id,
        "label": drilldown_id.replace("_", " ").title(),
        "available": False,
        "group_by": list(group_by),
        "dimension_labels": {column: DRILLDOWN_DIMENSION_LABELS.get(column, column.replace("_", " ").title()) for column in group_by},
        "source_dataset": source_dataset,
        "applied_filters": applied_filters,
        "logic_note": "Requested drill-down could not be generated because one or more dimensions are unavailable in the scoped data.",
        "unavailable_reason": reason,
        "rows": [],
    }


def _period_series(series: pd.Series, grain: str) -> pd.Series:
    timestamps = pd.to_datetime(series, errors="coerce")
    if grain == "day":
        return timestamps.dt.normalize()
    if grain == "week":
        return timestamps.dt.to_period("W-MON").dt.start_time
    if grain == "month":
        return timestamps.dt.to_period("M").dt.start_time
    raise ValueError(f"Unsupported date grain: {grain}")


def _build_drilldown_rows(
    df: pd.DataFrame,
    *,
    group_by: Sequence[str],
    metric_specs: Sequence[MetricSpec],
    source_dataset: str,
    applied_filters: Dict[str, object],
    logic_note: str,
    drilldown_id: str,
    label: str,
) -> Dict:
    # Generic grouped drill-down builder. It applies the exact same KPI
    # formulas to each group so grouped views stay consistent with top-level KPIs.
    missing = [column for column in group_by if column not in df.columns]
    if missing:
        return _build_drilldown_unavailable(
            drilldown_id=drilldown_id,
            group_by=group_by,
            source_dataset=source_dataset,
            applied_filters=applied_filters,
            reason=f"Missing columns: {', '.join(missing)}",
        )

    scoped = df.dropna(subset=list(group_by)).copy()
    if scoped.empty:
        return {
            "id": drilldown_id,
            "label": label,
            "available": True,
            "group_by": list(group_by),
            "dimension_labels": {column: DRILLDOWN_DIMENSION_LABELS.get(column, column.replace("_", " ").title()) for column in group_by},
            "source_dataset": source_dataset,
            "applied_filters": applied_filters,
            "logic_note": logic_note,
            "unavailable_reason": None,
            "formula_notes": [{"kpi": spec.name, "formula": spec.formula} for spec in metric_specs],
            "rows": [],
        }

    rows: List[Dict] = []
    grouped = scoped.groupby(list(group_by), dropna=False, sort=True)
    for keys, frame in grouped:
        if not isinstance(keys, tuple):
            keys = (keys,)
        row = {"row_count": int(len(frame)), "metrics": {}}
        for column, key in zip(group_by, keys):
            row[column] = _serialize_dimension_value(key)
        for spec in metric_specs:
            row["metrics"][spec.name] = _metric_snapshot(spec, frame, grain=f"grouped by {', '.join(group_by)}")
        rows.append(row)

    rows.sort(key=lambda item: tuple(_group_key_sortable(item.get(column)) for column in group_by))
    return {
        "id": drilldown_id,
        "label": label,
        "available": True,
        "group_by": list(group_by),
        "dimension_labels": {column: DRILLDOWN_DIMENSION_LABELS.get(column, column.replace("_", " ").title()) for column in group_by},
        "source_dataset": source_dataset,
        "applied_filters": applied_filters,
        "logic_note": logic_note,
        "unavailable_reason": None,
        "formula_notes": [{"kpi": spec.name, "formula": spec.formula} for spec in metric_specs],
        "rows": rows,
    }


def build_grouped_drilldown_table(
    df: pd.DataFrame,
    *,
    drilldown_id: str,
    group_by: Sequence[str],
    metric_specs: Sequence[MetricSpec],
    source_dataset: str,
    applied_filters: Dict[str, object],
    label: Optional[str] = None,
    logic_note: Optional[str] = None,
) -> Dict:
    return _build_drilldown_rows(
        df=df,
        group_by=group_by,
        metric_specs=metric_specs,
        source_dataset=source_dataset,
        applied_filters=applied_filters,
        logic_note=logic_note or "Grouped KPI view uses the same deterministic formulas as the top-level section KPIs.",
        drilldown_id=drilldown_id,
        label=label or drilldown_id.replace("_", " ").title(),
    )


def build_time_grain_drilldown(
    df: pd.DataFrame,
    *,
    drilldown_id: str,
    date_column: str,
    metric_specs: Sequence[MetricSpec],
    source_dataset: str,
    applied_filters: Dict[str, object],
    label: str,
) -> Dict:
    # Time drill-down exposes the same metric set at day/week/month grain from
    # one logical definition so consumers can pivot by period without new math.
    if date_column not in df.columns:
        return _build_drilldown_unavailable(
            drilldown_id=drilldown_id,
            group_by=[date_column],
            source_dataset=source_dataset,
            applied_filters=applied_filters,
            reason=f"Missing column: {date_column}",
        )

    prepared = df.copy()
    prepared[date_column] = pd.to_datetime(prepared[date_column], errors="coerce")
    if prepared[date_column].dropna().empty:
        return _build_drilldown_unavailable(
            drilldown_id=drilldown_id,
            group_by=[date_column],
            source_dataset=source_dataset,
            applied_filters=applied_filters,
            reason=f"Column {date_column} has no valid dates after filtering.",
        )

    grain_tables: Dict[str, Dict] = {}
    for grain in ["day", "week", "month"]:
        grain_column = f"{date_column}_{grain}"
        grain_df = prepared.copy()
        grain_df[grain_column] = _period_series(grain_df[date_column], grain)
        grain_tables[grain] = _build_drilldown_rows(
            df=grain_df,
            group_by=[grain_column],
            metric_specs=metric_specs,
            source_dataset=source_dataset,
            applied_filters=applied_filters,
            logic_note=f"Grouped by {grain} using the same deterministic formulas as the top-level section KPIs.",
            drilldown_id=f"{drilldown_id}_{grain}",
            label=f"{label} ({grain.title()})",
        )

    return {
        "id": drilldown_id,
        "label": label,
        "available": True,
        "group_by": [date_column],
        "dimension_labels": {date_column: DRILLDOWN_DIMENSION_LABELS.get(date_column, date_column.replace("_", " ").title())},
        "source_dataset": source_dataset,
        "applied_filters": applied_filters,
        "logic_note": "Time drill-down is available at day, week, and month grain using the same deterministic formulas as the top-level section KPIs.",
        "unavailable_reason": None,
        "available_grains": ["day", "week", "month"],
        "rows": grain_tables["day"]["rows"],
        "grain_tables": grain_tables,
    }


def _section_payload(name: str, kpis: List[Dict], drilldowns: Dict[str, Dict]) -> Dict:
    return {
        "name": name,
        "section": name.lower().replace(" ", "_"),
        "kpis": kpis,
        "drilldowns": drilldowns,
    }


def build_ranked_text_drilldown(
    *,
    drilldown_id: str,
    label: str,
    source_dataset: str,
    applied_filters: Dict[str, object],
    dimension_column: str,
    metric_label: str,
    formula: str,
    ranked_series: pd.Series,
    logic_note: str,
) -> Dict:
    # Some "top N" KPIs are narrative/ranked outputs rather than thresholded
    # performance KPIs, so they are represented as informational ranking rows.
    rows: List[Dict] = []
    for rank, (dimension_value, metric_value) in enumerate(ranked_series.items(), start=1):
        rows.append({
            "rank": rank,
            dimension_column: _serialize_dimension_value(dimension_value),
            "metrics": {
                metric_label: {
                    "name": metric_label,
                    "value": float(metric_value),
                    "display_value": format_value(float(metric_value), "qty"),
                    "unit": "qty",
                    "status": "info",
                    "target": None,
                    "target_display": None,
                    "formula": formula,
                    "source_table": source_dataset,
                    "grain": f"ranked by {dimension_column}",
                    "note": logic_note,
                }
            },
        })
    return {
        "id": drilldown_id,
        "label": label,
        "available": True,
        "group_by": ["rank", dimension_column],
        "dimension_labels": {
            "rank": "Rank",
            dimension_column: DRILLDOWN_DIMENSION_LABELS.get(dimension_column, dimension_column.replace("_", " ").title()),
        },
        "source_dataset": source_dataset,
        "applied_filters": applied_filters,
        "logic_note": logic_note,
        "unavailable_reason": None,
        "formula_notes": [{"kpi": metric_label, "formula": formula}],
        "rows": rows,
    }


def _inbound_metric_specs() -> List[MetricSpec]:
    # Formal KPI definitions for inbound operations.
    return [
        MetricSpec("Average Inbound Lead Time", "AVG(inbound_lead_time_days)", "inbound_parts", lambda frame: frame["inbound_lead_time_days"].mean() if not frame.empty else 0.0),
        MetricSpec("Receipts On-Time %", "COUNT(received_date <= expected_date) / COUNT(*)", "inbound_parts", lambda frame: float((frame["received_date"] <= frame["expected_date"]).mean()) if not frame.empty else 0.0),
        MetricSpec("Quantity Discrepancy %", "SUM(discrepancy_qty) / SUM(qty_ordered)", "inbound_parts", lambda frame: safe_divide(frame["discrepancy_qty"].sum(), frame["qty_ordered"].sum())),
        MetricSpec("Inbound Volume", "SUM(qty_received)", "inbound_parts", lambda frame: float(frame["qty_received"].sum())),
        MetricSpec("Late Receipt Count", "COUNT(received_date > expected_date)", "inbound_parts", lambda frame: float((frame["received_date"] > frame["expected_date"]).sum())),
    ]


def _outbound_metric_specs() -> List[MetricSpec]:
    # Formal KPI definitions for outbound service and fulfillment.
    return [
        MetricSpec("Fill Rate %", "SUM(qty_shipped) / SUM(qty_ordered)", "outbound_parts", lambda frame: safe_divide(frame["qty_shipped"].sum(), frame["qty_ordered"].sum())),
        MetricSpec("OTIF %", "SUM(otif_flag) / COUNT(*)", "outbound_parts", lambda frame: float(frame["otif_flag"].mean()) if not frame.empty else 0.0),
        MetricSpec("Backorder Rate %", "SUM(backorder_qty) / SUM(qty_ordered)", "outbound_parts", lambda frame: safe_divide(frame["backorder_qty"].sum(), frame["qty_ordered"].sum())),
        MetricSpec("Outbound Volume", "SUM(qty_shipped)", "outbound_parts", lambda frame: float(frame["qty_shipped"].sum())),
        MetricSpec("Late Shipment Count", "COUNT(shipped_date > promise_date)", "outbound_parts", lambda frame: float((frame["shipped_date"] > frame["promise_date"]).sum())),
    ]


def _inventory_metric_specs() -> List[MetricSpec]:
    # Formal KPI definitions for inventory health and aging.
    return [
        MetricSpec("Days of Supply", "AVG(days_of_supply)", "inventory_snapshot", lambda frame: frame["days_of_supply"].mean() if not frame.empty else 0.0),
        MetricSpec("Stockout Exposure %", "SUM(stockout_flag) / COUNT(*)", "inventory_snapshot", lambda frame: float(frame["stockout_flag"].mean()) if not frame.empty else 0.0),
        MetricSpec("Safety Stock Coverage %", "COUNT(available_qty >= safety_stock) / COUNT(*)", "inventory_snapshot", lambda frame: float((frame["available_qty"] >= frame["safety_stock"]).mean()) if not frame.empty else 0.0),
        MetricSpec("Aged Inventory % (>180d)", "COUNT(age_days > 180) / COUNT(*)", "inventory_snapshot", lambda frame: float((frame["age_days"] > 180).mean()) if not frame.empty else 0.0),
        MetricSpec("Average Inventory Age", "AVG(age_days)", "inventory_snapshot", lambda frame: frame["age_days"].mean() if not frame.empty else 0.0),
    ]


def _warehouse_productivity_metric_specs() -> List[MetricSpec]:
    # Formal KPI definitions for warehouse throughput and service execution.
    return [
        MetricSpec("Lines Picked per Labor-Hour", "SUM(lines_picked) / SUM(labor_hours)", "warehouse_productivity", lambda frame: safe_divide(frame["lines_picked"].sum(), frame["labor_hours"].sum())),
        MetricSpec("Orders Processed per Labor-Hour", "SUM(orders_processed) / SUM(labor_hours)", "warehouse_productivity", lambda frame: safe_divide(frame["orders_processed"].sum(), frame["labor_hours"].sum())),
        MetricSpec("Orders per Day", "SUM(orders_processed) / COUNT(DISTINCT date)", "warehouse_productivity", lambda frame: safe_divide(frame["orders_processed"].sum(), frame["date"].nunique() if "date" in frame.columns else 0.0)),
        MetricSpec("SLA Adherence %", "AVG(sla_adherence_pct)", "warehouse_productivity", lambda frame: frame["sla_adherence_pct"].mean() if not frame.empty else 0.0),
        MetricSpec("Equipment Utilization %", "AVG(equipment_utilization_pct)", "warehouse_productivity", lambda frame: frame["equipment_utilization_pct"].mean() if not frame.empty else 0.0),
        MetricSpec("Touches per Order", "AVG(touches_per_order)", "warehouse_productivity", lambda frame: frame["touches_per_order"].mean() if not frame.empty else 0.0),
    ]


def _employee_productivity_metric_specs() -> List[MetricSpec]:
    # Formal KPI definitions for labor productivity and quality.
    return [
        MetricSpec("Picks per Person per Hour", "SUM(picks) / SUM(hours_worked)", "employee_productivity", lambda frame: safe_divide(frame["picks"].sum(), frame["hours_worked"].sum())),
        MetricSpec("Error Rate %", "SUM(errors) / SUM(tasks_completed)", "employee_productivity", lambda frame: safe_divide(frame["errors"].sum(), frame["tasks_completed"].sum())),
        MetricSpec("Rework Rate %", "SUM(rework) / SUM(tasks_completed)", "employee_productivity", lambda frame: safe_divide(frame["rework"].sum(), frame["tasks_completed"].sum())),
        MetricSpec("Overtime %", "SUM(overtime_hours) / SUM(hours_worked + overtime_hours)", "employee_productivity", lambda frame: safe_divide(frame["overtime_hours"].sum(), (frame["hours_worked"] + frame["overtime_hours"]).sum())),
        MetricSpec("Average Tasks per Employee", "AVG(tasks_completed)", "employee_productivity", lambda frame: frame["tasks_completed"].mean() if not frame.empty else 0.0),
    ]


def _build_section_drilldowns(
    *,
    section_name: str,
    drilldown_df: pd.DataFrame,
    metric_specs: Sequence[MetricSpec],
    applied_filters: Dict[str, object],
) -> Dict[str, Dict]:
    # Section drill-downs are configuration-driven so dimensions can be added
    # in config without rewriting each section calculator.
    config = DRILLDOWN_CONFIG[section_name]
    source_dataset = str(config["source_dataset"])
    dimensions = config["dimensions"]  # type: ignore[assignment]
    drilldowns: Dict[str, Dict] = {}

    for drilldown_id in config["dimension_order"]:  # type: ignore[index]
        group_by = list(dimensions[drilldown_id])  # type: ignore[index]
        if drilldown_id == "by_date":
            drilldowns[drilldown_id] = build_time_grain_drilldown(
                drilldown_df,
                drilldown_id=drilldown_id,
                date_column=group_by[0],
                metric_specs=metric_specs,
                source_dataset=source_dataset,
                applied_filters=applied_filters,
                label="By Date",
            )
        else:
            drilldowns[drilldown_id] = build_grouped_drilldown_table(
                drilldown_df,
                drilldown_id=drilldown_id,
                group_by=group_by,
                metric_specs=metric_specs,
                source_dataset=source_dataset,
                applied_filters=applied_filters,
                label=drilldown_id.replace("_", " ").title(),
            )
    return drilldowns


def _attach_raw_detail(kpis: List[Dict], raw_detail: Dict) -> None:
    # Every KPI in a section points to the same scoped raw-detail payload.
    for kpi in kpis:
        kpi["raw_detail"] = raw_detail


def compute_inbound_kpis(
    inbound: pd.DataFrame,
    inventory: pd.DataFrame,
    start: pd.Timestamp,
    end: pd.Timestamp,
    sku_families: Optional[Sequence[str]] = None,
    allowed_parts: Optional[Sequence[str]] = None,
    warehouses: Optional[Sequence[str]] = None,
) -> Dict:
    # Inbound is network-level in the source data, so warehouse filters are only
    # carried for traceability and do not slice the underlying rows.
    df = filter_period(inbound, "received_date", start, end)
    df = filter_sku_families(df, sku_families, allowed_parts)
    df = df.copy()
    # Store a reusable flag so raw detail and grouped views can expose the
    # on-time receipt logic explicitly.
    df["receipt_on_time_flag"] = (df["received_date"] <= df["expected_date"]).astype(int)
    metric_specs = _inbound_metric_specs()
    metric_lookup = {spec.name: spec for spec in metric_specs}

    lead_time = metric_lookup["Average Inbound Lead Time"].compute(df)
    on_time_pct = metric_lookup["Receipts On-Time %"].compute(df)
    discrepancy_pct = metric_lookup["Quantity Discrepancy %"].compute(df)
    volume = metric_lookup["Inbound Volume"].compute(df)
    late_count = metric_lookup["Late Receipt Count"].compute(df)

    late_supplier_volume = (
        df.loc[df["received_date"] > df["expected_date"]]
        .groupby("supplier_name", dropna=False)["qty_ordered"]
        .sum()
        .sort_values(ascending=False)
        .head(5)
    )
    # Prefer comments that name a concrete supplier problem over generic status
    # wording because leadership action is easier when the likely driver is known.
    top_delaying_suppliers = (
        ", ".join(f"{supplier} ({int(volume):,})" for supplier, volume in late_supplier_volume.items())
        if not late_supplier_volume.empty
        else "No late suppliers in period"
    )
    top_supplier_names = _top_labels(late_supplier_volume)
    lead_time_comment = _generic_comment(lead_time, THRESHOLDS["Average Inbound Lead Time"])
    on_time_comment = (
        f"Late receipts from {' & '.join(top_supplier_names)} reduced on-time performance"
        if top_supplier_names
        else _generic_comment(on_time_pct, THRESHOLDS["Receipts On-Time %"])
    )
    discrepancy_comment = (
        f"Mismatch volume is concentrated with {' & '.join(top_supplier_names)}"
        if top_supplier_names and discrepancy_pct > THRESHOLDS["Quantity Discrepancy %"].target
        else _generic_comment(discrepancy_pct, THRESHOLDS["Quantity Discrepancy %"])
    )
    volume_comment = "Inbound volume processed in the selected period"
    late_count_comment = (
        f"Late receipts concentrated across {' & '.join(top_supplier_names)}"
        if top_supplier_names
        else "No late receipt concentration detected"
    )

    kpis = [
        build_kpi("Average Inbound Lead Time", lead_time, metric_lookup["Average Inbound Lead Time"].formula, "inbound_parts", "reporting period", note=lead_time_comment),
        build_kpi("Receipts On-Time %", on_time_pct, metric_lookup["Receipts On-Time %"].formula, "inbound_parts", "reporting period", note=on_time_comment),
        build_kpi("Quantity Discrepancy %", discrepancy_pct, metric_lookup["Quantity Discrepancy %"].formula, "inbound_parts", "reporting period", note=discrepancy_comment),
        build_kpi("Inbound Volume", volume, metric_lookup["Inbound Volume"].formula, "inbound_parts", "reporting period", note=volume_comment),
        build_kpi("Late Receipt Count", late_count, metric_lookup["Late Receipt Count"].formula, "inbound_parts", "reporting period", note=late_count_comment),
        build_kpi(
            "Top 5 Delaying Suppliers",
            float(len(late_supplier_volume)),
            "TOP 5 supplier_name BY SUM(qty_ordered) WHERE received_date > expected_date",
            "inbound_parts",
            "reporting period",
            note="Ranked by late ordered quantity in the selected period.",
            display_override=top_delaying_suppliers,
        ),
    ]
    _attach_raw_detail(
        kpis,
        build_raw_detail(
            df,
            section_name="Inbound",
            source_dataset="inbound_parts",
            applied_filters=_serialize_filters(start=start, end=end, warehouses=warehouses, sku_families=sku_families),
            sort_by=["received_date", "expected_date"],
            logic_note="Raw detail contains the filtered inbound receipt records used for the selected KPI.",
        ),
    )

    drilldowns = _build_section_drilldowns(
        section_name="Inbound",
        drilldown_df=_enrich_inbound_outbound_dimensions(df, inventory, date_alias="receipt_date"),
        metric_specs=metric_specs,
        applied_filters=_serialize_filters(start=start, end=end, warehouses=warehouses, sku_families=sku_families),
    )
    drilldowns["by_late_supplier_rank"] = build_ranked_text_drilldown(
        drilldown_id="by_late_supplier_rank",
        label="Late Suppliers Ranking",
        source_dataset="inbound_parts",
        applied_filters=_serialize_filters(start=start, end=end, warehouses=warehouses, sku_families=sku_families),
        dimension_column="supplier_name",
        metric_label="Late Ordered Quantity",
        formula="TOP supplier_name BY SUM(qty_ordered) WHERE received_date > expected_date",
        ranked_series=late_supplier_volume,
        logic_note="Ranks suppliers by ordered quantity tied to late receipts in the selected period.",
    )
    return _section_payload("Inbound", kpis, drilldowns)


def compute_outbound_kpis(
    outbound: pd.DataFrame,
    inventory: pd.DataFrame,
    start: pd.Timestamp,
    end: pd.Timestamp,
    sku_families: Optional[Sequence[str]] = None,
    allowed_parts: Optional[Sequence[str]] = None,
    warehouses: Optional[Sequence[str]] = None,
) -> Dict:
    # Outbound is also network-level in the current source model; warehouse
    # scope is preserved in metadata but does not slice the transactional rows.
    df = filter_period(outbound, "shipped_date", start, end)
    df = filter_sku_families(df, sku_families, allowed_parts)
    df = df.copy()
    # Expose lateness explicitly for raw detail and downstream drill-down use.
    df["late_shipment_flag"] = (df["shipped_date"] > df["promise_date"]).astype(int)
    metric_specs = _outbound_metric_specs()
    metric_lookup = {spec.name: spec for spec in metric_specs}

    fill_rate = metric_lookup["Fill Rate %"].compute(df)
    otif = metric_lookup["OTIF %"].compute(df)
    backorder_rate = metric_lookup["Backorder Rate %"].compute(df)
    volume = metric_lookup["Outbound Volume"].compute(df)
    late_shipments = metric_lookup["Late Shipment Count"].compute(df)

    top_backorder_skus = (
        df.groupby("part_number", dropna=False)["backorder_qty"]
        .sum()
        .sort_values(ascending=False)
        .head(10)
    )
    # When possible, comments point to concrete impacted SKUs or orders rather
    # than generic threshold text.
    backorder_sku_display = (
        ", ".join(f"{part} ({int(qty):,})" for part, qty in top_backorder_skus.items())
        if not top_backorder_skus.empty
        else "No backorders in period"
    )
    top_sku_names = _top_labels(top_backorder_skus)
    late_orders = (
        df.loc[df["shipped_date"] > df["promise_date"], "order_number"]
        .dropna()
        .astype(str)
        .drop_duplicates()
        .head(2)
        .tolist()
    )
    fill_rate_comment = (
        f"{top_sku_names[0]} backorders impacted fill rate"
        if top_sku_names and backorder_rate > 0
        else _generic_comment(fill_rate, THRESHOLDS["Fill Rate %"])
    )
    otif_comment = (
        f"Late shipments on {' & '.join(late_orders)}"
        if late_orders
        else _generic_comment(otif, THRESHOLDS["OTIF %"])
    )
    backorder_comment = (
        f"Backorders concentrated in {' & '.join(top_sku_names[:2])}"
        if top_sku_names
        else _generic_comment(backorder_rate, THRESHOLDS["Backorder Rate %"])
    )
    volume_comment = "Outbound volume shipped in the selected period"
    late_shipments_comment = (
        f"Late shipments include {' & '.join(late_orders)}"
        if late_orders
        else "No late shipment concentration detected"
    )

    kpis = [
        build_kpi("Fill Rate %", fill_rate, metric_lookup["Fill Rate %"].formula, "outbound_parts", "reporting period", note=fill_rate_comment),
        build_kpi("OTIF %", otif, metric_lookup["OTIF %"].formula, "outbound_parts", "reporting period", note=otif_comment),
        build_kpi("Backorder Rate %", backorder_rate, metric_lookup["Backorder Rate %"].formula, "outbound_parts", "reporting period", note=backorder_comment),
        build_kpi("Outbound Volume", volume, metric_lookup["Outbound Volume"].formula, "outbound_parts", "reporting period", note=volume_comment),
        build_kpi("Late Shipment Count", late_shipments, metric_lookup["Late Shipment Count"].formula, "outbound_parts", "reporting period", note=late_shipments_comment),
        build_kpi(
            "Top 10 SKUs by Backorder",
            float(len(top_backorder_skus)),
            "TOP 10 part_number BY SUM(backorder_qty)",
            "outbound_parts",
            "reporting period",
            note="Ranked by backordered quantity in the selected period.",
            display_override=backorder_sku_display,
        ),
    ]
    _attach_raw_detail(
        kpis,
        build_raw_detail(
            df,
            section_name="Outbound",
            source_dataset="outbound_parts",
            applied_filters=_serialize_filters(start=start, end=end, warehouses=warehouses, sku_families=sku_families),
            sort_by=["shipped_date", "promise_date"],
            logic_note="Raw detail contains the filtered outbound shipment records used for the selected KPI.",
        ),
    )

    drilldowns = _build_section_drilldowns(
        section_name="Outbound",
        drilldown_df=_enrich_inbound_outbound_dimensions(df, inventory, date_alias="ship_date"),
        metric_specs=metric_specs,
        applied_filters=_serialize_filters(start=start, end=end, warehouses=warehouses, sku_families=sku_families),
    )
    drilldowns["by_backorder_sku_rank"] = build_ranked_text_drilldown(
        drilldown_id="by_backorder_sku_rank",
        label="Backorder SKU Ranking",
        source_dataset="outbound_parts",
        applied_filters=_serialize_filters(start=start, end=end, warehouses=warehouses, sku_families=sku_families),
        dimension_column="part_number",
        metric_label="Backorder Quantity",
        formula="TOP part_number BY SUM(backorder_qty)",
        ranked_series=top_backorder_skus,
        logic_note="Ranks part numbers by backordered quantity in the selected period.",
    )
    return _section_payload("Outbound", kpis, drilldowns)


def compute_inventory_kpis(
    inventory: pd.DataFrame,
    outbound: pd.DataFrame,
    start: pd.Timestamp,
    end: pd.Timestamp,
    warehouses: Optional[Sequence[str]] = None,
    sku_families: Optional[Sequence[str]] = None,
    allowed_parts: Optional[Sequence[str]] = None,
) -> Dict:
    # Inventory is warehouse-aware, so warehouse filters are applied directly to
    # the snapshot rows before KPI calculation.
    df = filter_period(inventory, "snapshot_date", start, end)
    df = filter_warehouses(df, warehouses)
    df = filter_sku_families(df, sku_families)
    df = df.copy()
    # Materialize this flag once so it can be reused in raw detail and grouped
    # views without re-expressing the comparison logic elsewhere.
    df["below_safety_stock_flag"] = (df["available_qty"] < df["safety_stock"]).astype(int)
    outbound_df = filter_period(outbound, "shipped_date", start, end)
    outbound_df = filter_sku_families(outbound_df, sku_families, allowed_parts)
    metric_specs = _inventory_metric_specs()
    metric_lookup = {spec.name: spec for spec in metric_specs}

    dos = metric_lookup["Days of Supply"].compute(df)
    stockout = metric_lookup["Stockout Exposure %"].compute(df)
    safety_coverage = metric_lookup["Safety Stock Coverage %"].compute(df)
    aged_pct = metric_lookup["Aged Inventory % (>180d)"].compute(df)
    avg_age = metric_lookup["Average Inventory Age"].compute(df)

    periods_in_month = max(end.days_in_month, 1)
    # Inventory turns is a proxy in this assignment: shipped quantity is used as
    # issue velocity because richer finance valuation data is not available.
    avg_daily_issues = safe_divide(outbound_df["qty_shipped"].sum(), periods_in_month)
    avg_on_hand = float(df["on_hand_qty"].mean()) if not df.empty else 0.0
    inventory_turns = safe_divide(avg_daily_issues * 365, avg_on_hand)
    top_stockout_row = None
    if not df.empty:
        stockout_rows = df.loc[df["stockout_flag"] > 0]
        if not stockout_rows.empty:
            top_stockout_row = stockout_rows.sort_values(["available_qty", "on_hand_qty"], ascending=[True, True]).iloc[0]
    if "unit_cost" in df.columns:
        aged_inventory_value = float(df.loc[df["age_days"] > 180, "on_hand_qty"].mul(df.loc[df["age_days"] > 180, "unit_cost"]).sum())
        aged_value_note = "Computed from on_hand_qty * unit_cost for inventory older than 180 days."
    else:
        # Fall back to an operational quantity proxy when financial valuation is missing.
        aged_inventory_value = float(df.loc[df["age_days"] > 180, "on_hand_qty"].sum()) if not df.empty else 0.0
        aged_value_note = "Proxy uses aged on-hand quantity because unit_cost is not present in the current data model."
    turns_note = "Proxy uses annualized shipped quantity over average on-hand quantity because finance valuation fields are not present."
    if sku_families:
        scope_label = ", ".join(sku_families[:2])
    elif "part_number" in df.columns and not df.empty:
        dominant_part = (
            df.groupby("part_number", dropna=False)["on_hand_qty"]
            .sum()
            .sort_values(ascending=False)
            .head(1)
            .index.tolist()
        )
        scope_label = str(dominant_part[0]) if dominant_part else "inventory scope"
    else:
        scope_label = "inventory scope"
    dos_comment = (
        f"Healthy for {scope_label}"
        if evaluate_status(dos, THRESHOLDS["Days of Supply"]) == "green"
        else _generic_comment(dos, THRESHOLDS["Days of Supply"])
    )
    if top_stockout_row is not None:
        stockout_comment = f"{top_stockout_row['part_number']} stock-out risk at {top_stockout_row['warehouse_id']}"
        safety_stock_comment = f"{top_stockout_row['part_number']} is below safety stock at {top_stockout_row['warehouse_id']}"
    else:
        stockout_comment = _generic_comment(stockout, THRESHOLDS["Stockout Exposure %"])
        safety_stock_comment = _generic_comment(safety_coverage, THRESHOLDS["Safety Stock Coverage %"])
    aged_pct_comment = _generic_comment(aged_pct, THRESHOLDS["Aged Inventory % (>180d)"])
    avg_age_comment = "Average inventory age across the selected scope"

    kpis = [
        build_kpi("Days of Supply", dos, metric_lookup["Days of Supply"].formula, "inventory_snapshot", "reporting period", note=dos_comment),
        build_kpi("Stockout Exposure %", stockout, metric_lookup["Stockout Exposure %"].formula, "inventory_snapshot", "reporting period", note=stockout_comment),
        build_kpi("Safety Stock Coverage %", safety_coverage, metric_lookup["Safety Stock Coverage %"].formula, "inventory_snapshot", "reporting period", note=safety_stock_comment),
        build_kpi("Aged Inventory % (>180d)", aged_pct, metric_lookup["Aged Inventory % (>180d)"].formula, "inventory_snapshot", "reporting period", note=aged_pct_comment),
        build_kpi("Average Inventory Age", avg_age, metric_lookup["Average Inventory Age"].formula, "inventory_snapshot", "reporting period", note=avg_age_comment),
        build_kpi("Inventory Turns", inventory_turns, "ANNUALIZED SUM(qty_shipped) / AVG(on_hand_qty)", "inventory_snapshot + outbound_parts", "reporting period", note=turns_note),
        build_kpi(
            "Aged Inventory Value >180d",
            aged_inventory_value,
            "SUM(on_hand_qty * unit_cost) WHERE age_days > 180; proxy to SUM(on_hand_qty) when unit_cost is unavailable",
            "inventory_snapshot",
            "reporting period",
            note=aged_value_note,
        ),
    ]
    _attach_raw_detail(
        kpis,
        build_raw_detail(
            df,
            section_name="Inventory",
            source_dataset="inventory_snapshot",
            applied_filters=_serialize_filters(start=start, end=end, warehouses=warehouses, sku_families=sku_families),
            sort_by=["snapshot_date", "warehouse_id"],
            logic_note="Raw detail contains the filtered inventory snapshot rows used for the selected KPI.",
        ),
    )

    drilldowns = _build_section_drilldowns(
        section_name="Inventory",
        drilldown_df=_prepare_inventory_drilldown_df(df),
        metric_specs=metric_specs,
        applied_filters=_serialize_filters(start=start, end=end, warehouses=warehouses, sku_families=sku_families),
    )
    return _section_payload("Inventory", kpis, drilldowns)


def compute_warehouse_productivity_kpis(
    warehouse_df: pd.DataFrame,
    start: pd.Timestamp,
    end: pd.Timestamp,
    warehouses: Optional[Sequence[str]] = None,
) -> Dict:
    # Warehouse productivity is naturally warehouse-grained, so this section
    # supports both network-wide and scoped warehouse views.
    df = filter_period(warehouse_df, "date", start, end)
    df = filter_warehouses(df, warehouses)
    metric_specs = _warehouse_productivity_metric_specs()
    metric_lookup = {spec.name: spec for spec in metric_specs}

    lines_per_hour = metric_lookup["Lines Picked per Labor-Hour"].compute(df)
    orders_per_hour = metric_lookup["Orders Processed per Labor-Hour"].compute(df)
    orders_per_day = metric_lookup["Orders per Day"].compute(df)
    sla = metric_lookup["SLA Adherence %"].compute(df)
    equip_util = metric_lookup["Equipment Utilization %"].compute(df)
    touches = metric_lookup["Touches per Order"].compute(df)

    weakest_shift_row = None
    if not df.empty:
        # Use the weakest shift as the comment driver so narrative points to an
        # operationally actionable bottleneck.
        weakest_shift_row = df.assign(
            lines_per_hour_row=df["lines_picked"] / df["labor_hours"].replace(0, pd.NA),
            orders_per_hour_row=df["orders_processed"] / df["labor_hours"].replace(0, pd.NA),
        ).sort_values(["sla_adherence_pct", "lines_per_hour_row"], ascending=[True, True], na_position="last").iloc[0]
    if weakest_shift_row is not None and pd.notna(weakest_shift_row.get("warehouse_id")):
        lines_comment = f"{weakest_shift_row['warehouse_id']} {weakest_shift_row['shift']} shift throughput is below plan"
        orders_per_hour_comment = f"{weakest_shift_row['warehouse_id']} {weakest_shift_row['shift']} shift order flow is below plan"
        sla_comment = f"{weakest_shift_row['warehouse_id']} {weakest_shift_row['shift']} shift missed SLA commitments"
    else:
        lines_comment = _generic_comment(lines_per_hour, THRESHOLDS["Lines Picked per Labor-Hour"])
        orders_per_hour_comment = _generic_comment(orders_per_hour, THRESHOLDS["Orders Processed per Labor-Hour"])
        sla_comment = _generic_comment(sla, THRESHOLDS["SLA Adherence %"])
    orders_per_day_comment = "Average daily order volume processed"
    if weakest_shift_row is not None and pd.notna(weakest_shift_row.get("equipment_utilization_pct")):
        equipment_comment = f"Equipment utilization at {weakest_shift_row['warehouse_id']} averaged {weakest_shift_row['equipment_utilization_pct'] * 100:.1f}%"
    else:
        equipment_comment = _generic_comment(equip_util, THRESHOLDS["Equipment Utilization %"])
    touches_comment = _generic_comment(touches, THRESHOLDS["Touches per Order"])

    kpis = [
        build_kpi("Lines Picked per Labor-Hour", lines_per_hour, metric_lookup["Lines Picked per Labor-Hour"].formula, "warehouse_productivity", "reporting period", note=lines_comment),
        build_kpi("Orders Processed per Labor-Hour", orders_per_hour, metric_lookup["Orders Processed per Labor-Hour"].formula, "warehouse_productivity", "reporting period", note=orders_per_hour_comment),
        build_kpi("Orders per Day", orders_per_day, metric_lookup["Orders per Day"].formula, "warehouse_productivity", "reporting period", note=orders_per_day_comment),
        build_kpi("SLA Adherence %", sla, metric_lookup["SLA Adherence %"].formula, "warehouse_productivity", "reporting period", note=sla_comment),
        build_kpi("Equipment Utilization %", equip_util, metric_lookup["Equipment Utilization %"].formula, "warehouse_productivity", "reporting period", note=equipment_comment),
        build_kpi("Touches per Order", touches, metric_lookup["Touches per Order"].formula, "warehouse_productivity", "reporting period", note=touches_comment),
    ]
    _attach_raw_detail(
        kpis,
        build_raw_detail(
            df,
            section_name="Warehouse Productivity",
            source_dataset="warehouse_productivity",
            applied_filters=_serialize_filters(start=start, end=end, warehouses=warehouses, sku_families=None),
            sort_by=["date", "warehouse_id"],
            logic_note="Raw detail contains the filtered warehouse productivity rows used for the selected KPI.",
        ),
    )

    drilldowns = _build_section_drilldowns(
        section_name="Warehouse Productivity",
        drilldown_df=_prepare_warehouse_productivity_drilldown_df(df),
        metric_specs=metric_specs,
        applied_filters=_serialize_filters(start=start, end=end, warehouses=warehouses, sku_families=None),
    )
    return _section_payload("Warehouse Productivity", kpis, drilldowns)


def compute_employee_productivity_kpis(
    employee_df: pd.DataFrame,
    start: pd.Timestamp,
    end: pd.Timestamp,
    warehouses: Optional[Sequence[str]] = None,
) -> Dict:
    # Employee productivity is warehouse-aware and can be recomputed for chatbot
    # comparisons at narrower scope.
    df = filter_period(employee_df, "date", start, end)
    df = filter_warehouses(df, warehouses)
    metric_specs = _employee_productivity_metric_specs()
    metric_lookup = {spec.name: spec for spec in metric_specs}

    picks_per_hour = metric_lookup["Picks per Person per Hour"].compute(df)
    error_rate = metric_lookup["Error Rate %"].compute(df)
    rework_rate = metric_lookup["Rework Rate %"].compute(df)
    overtime = metric_lookup["Overtime %"].compute(df)
    avg_tasks = metric_lookup["Average Tasks per Employee"].compute(df)

    top_error_row = None
    if not df.empty:
        # Surface the highest-error employee/shift combination when present so
        # the note is more actionable than a generic "red" explanation.
        error_rows = df.loc[df["errors"] > 0].sort_values(["errors", "rework", "overtime_hours"], ascending=[False, False, False])
        if not error_rows.empty:
            top_error_row = error_rows.iloc[0]
    high_overtime_row = None
    if not df.empty:
        overtime_rows = df.loc[df["overtime_hours"] > 0].sort_values(["overtime_hours", "errors"], ascending=[False, False])
        if not overtime_rows.empty:
            high_overtime_row = overtime_rows.iloc[0]
    picks_comment = _generic_comment(picks_per_hour, THRESHOLDS["Picks per Person per Hour"])
    if top_error_row is not None:
        error_comment = f"{top_error_row['shift']} shift errors from {top_error_row['employee_id']}"
    else:
        error_comment = _generic_comment(error_rate, THRESHOLDS["Error Rate %"])
    rework_comment = _generic_comment(rework_rate, THRESHOLDS["Rework Rate %"])
    if high_overtime_row is not None:
        overtime_comment = f"{high_overtime_row['shift']} shift overtime is highest for {high_overtime_row['employee_id']}"
    else:
        overtime_comment = _generic_comment(overtime, THRESHOLDS["Overtime %"])
    avg_tasks_comment = "Average task load per employee record"

    kpis = [
        build_kpi("Picks per Person per Hour", picks_per_hour, metric_lookup["Picks per Person per Hour"].formula, "employee_productivity", "reporting period", note=picks_comment),
        build_kpi("Error Rate %", error_rate, metric_lookup["Error Rate %"].formula, "employee_productivity", "reporting period", note=error_comment),
        build_kpi("Rework Rate %", rework_rate, metric_lookup["Rework Rate %"].formula, "employee_productivity", "reporting period", note=rework_comment),
        build_kpi("Overtime %", overtime, metric_lookup["Overtime %"].formula, "employee_productivity", "reporting period", note=overtime_comment),
        build_kpi("Average Tasks per Employee", avg_tasks, metric_lookup["Average Tasks per Employee"].formula, "employee_productivity", "reporting period", note=avg_tasks_comment),
    ]
    _attach_raw_detail(
        kpis,
        build_raw_detail(
            df,
            section_name="Employee Productivity",
            source_dataset="employee_productivity",
            applied_filters=_serialize_filters(start=start, end=end, warehouses=warehouses, sku_families=None),
            sort_by=["date", "employee_id"],
            logic_note="Raw detail contains the filtered employee productivity rows used for the selected KPI.",
        ),
    )

    drilldowns = _build_section_drilldowns(
        section_name="Employee Productivity",
        drilldown_df=_prepare_employee_productivity_drilldown_df(df),
        metric_specs=metric_specs,
        applied_filters=_serialize_filters(start=start, end=end, warehouses=warehouses, sku_families=None),
    )
    return _section_payload("Employee Productivity", kpis, drilldowns)


def compute_all_kpis(
    datasets: Dict[str, pd.DataFrame],
    start: pd.Timestamp,
    end: pd.Timestamp,
    warehouses: Optional[Sequence[str]] = None,
    sku_families: Optional[Sequence[str]] = None,
) -> Tuple[List[Dict], List[Dict]]:
    # Aggregate all business domains into one nested section payload plus one
    # flat table payload for summary/export use.
    allowed_parts = None
    if sku_families:
        # Inbound/outbound datasets may not carry sku_family directly, so derive
        # the allowed part universe from inventory before applying family scope.
        inventory = datasets["inventory_snapshot"]
        allowed_parts = sorted(
            inventory[inventory["sku_family"].isin(sku_families)]["part_number"].dropna().astype(str).unique().tolist()
        )

    sections = [
        compute_inbound_kpis(datasets["inbound_parts"], datasets["inventory_snapshot"], start, end, sku_families, allowed_parts, warehouses),
        compute_outbound_kpis(datasets["outbound_parts"], datasets["inventory_snapshot"], start, end, sku_families, allowed_parts, warehouses),
        compute_inventory_kpis(datasets["inventory_snapshot"], datasets["outbound_parts"], start, end, warehouses, sku_families, allowed_parts),
        compute_warehouse_productivity_kpis(datasets["warehouse_productivity"], start, end, warehouses),
        compute_employee_productivity_kpis(datasets["employee_productivity"], start, end, warehouses),
    ]

    kpi_table: List[Dict] = []
    for section in sections:
        for kpi in section["kpis"]:
            kpi_table.append({
                "domain": section["name"],
                "kpi": kpi["name"],
                "value": kpi["value"],
                "display_value": kpi["display_value"],
                "unit": kpi["unit"],
                "target": kpi["target"],
                "target_display": kpi["target_display"],
                "status": kpi["status"],
                "trend_note": kpi.get("note", ""),
            })
    return sections, kpi_table


def get_kpi_lookup(sections: List[Dict]) -> Dict[str, Dict]:
    lookup = {}
    for section in sections:
        for kpi in section["kpis"]:
            lookup[kpi["name"]] = kpi
    return lookup
