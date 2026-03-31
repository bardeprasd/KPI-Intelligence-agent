"""
Deterministic KPI calculation logic for all operational domains.

This file sits in the KPI computation layer of the project. It filters datasets
to the requested scope, calculates supply-chain and warehouse metrics, evaluates
status against configured thresholds, and packages results into a common schema.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Sequence, Tuple

import pandas as pd

from .config import THRESHOLDS, KPIThreshold


def safe_divide(numerator: float, denominator: float) -> float:
    # ================================
    # Function: safe_divide
    # Purpose: Prevents divide-by-zero errors in KPI calculations.
    # Inputs:
    #   - numerator (float)
    #   - denominator (float)
    # Output:
    #   - float result, or 0.0 when the denominator is zero
    # ================================
    return float(numerator / denominator) if denominator not in (0, 0.0) else 0.0


def filter_period(df: pd.DataFrame, date_col: str, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    # ================================
    # Function: filter_period
    # Purpose: Restricts a dataset to the selected reporting date range.
    # Inputs:
    #   - df (pd.DataFrame): source dataset
    #   - date_col (str): timestamp column used for filtering
    #   - start (pd.Timestamp): inclusive range start
    #   - end (pd.Timestamp): inclusive range end
    # Output:
    #   - pd.DataFrame limited to the requested period
    # ================================
    return df[(df[date_col] >= start) & (df[date_col] <= end)].copy()


def filter_warehouses(df: pd.DataFrame, warehouses: Optional[Sequence[str]]) -> pd.DataFrame:
    # ================================
    # Function: filter_warehouses
    # Purpose: Applies optional warehouse scoping when the dataset supports it.
    # Inputs:
    #   - df (pd.DataFrame): source dataset
    #   - warehouses (Optional[Sequence[str]]): requested warehouse IDs
    # Output:
    #   - pd.DataFrame filtered to the requested warehouses
    # ================================
    if not warehouses or "warehouse_id" not in df.columns:
        return df
    return df[df["warehouse_id"].isin(warehouses)].copy()


def filter_sku_families(df: pd.DataFrame, sku_families: Optional[Sequence[str]], allowed_parts: Optional[Sequence[str]] = None) -> pd.DataFrame:
    # ================================
    # Function: filter_sku_families
    # Purpose: Applies SKU-family scoping to datasets with or without family columns.
    # Inputs:
    #   - df (pd.DataFrame): source dataset
    #   - sku_families (Optional[Sequence[str]]): requested SKU families
    #   - allowed_parts (Optional[Sequence[str]]): derived part-number fallback
    # Output:
    #   - pd.DataFrame filtered to the requested product scope
    # Important Logic:
    #   - Transaction tables without `sku_family` are scoped through the part
    #     numbers observed in the filtered inventory snapshot
    # ================================
    if not sku_families:
        return df
    if "sku_family" in df.columns:
        return df[df["sku_family"].isin(sku_families)].copy()
    if allowed_parts is not None and "part_number" in df.columns:
        return df[df["part_number"].isin(allowed_parts)].copy()
    return df


def evaluate_status(value: float, threshold: KPIThreshold) -> str:
    # ================================
    # Function: evaluate_status
    # Purpose: Converts a KPI value into a business health status.
    # Inputs:
    #   - value (float): computed KPI value
    #   - threshold (KPIThreshold): threshold rule from config
    # Output:
    #   - str status: green, amber, red, or info
    # ================================
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
    # ================================
    # Function: format_value
    # Purpose: Formats raw KPI values for human-readable outputs.
    # Inputs:
    #   - value (float): raw KPI value
    #   - unit (str): unit type such as pct, days, qty, or count
    # Output:
    #   - str display value
    # ================================
    if unit == "pct":
        return f"{value * 100:.1f}%"
    if unit == "days":
        return f"{value:.1f} days"
    if unit == "qty":
        return f"{value:,.0f}"
    if unit == "count":
        return f"{value:,.0f}"
    if unit == "text":
        return str(value)
    return f"{value:.2f}"


def format_target(threshold: KPIThreshold) -> Optional[str]:
    # ================================
    # Function: format_target
    # Purpose: Formats the target rule shown beside a KPI.
    # Inputs:
    #   - threshold (KPIThreshold): threshold configuration
    # Output:
    #   - Optional[str] display-ready target text
    # ================================
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
    # Returns the first few non-empty index labels from a ranked Series.
    labels: List[str] = []
    for item in series.index.tolist()[:count]:
        text = str(item).strip()
        if text and text.lower() != "nan":
            labels.append(text)
    return labels


def _generic_comment(value: float, threshold: KPIThreshold) -> str:
    # Produces a short deterministic comment from the KPI threshold logic.
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
    # ================================
    # Function: build_kpi
    # Purpose: Packages a computed KPI into the standard output structure.
    # Inputs:
    #   - name (str): KPI name matching threshold config
    #   - value (float): computed KPI value
    #   - formula (str): traceable calculation expression
    #   - source_table (str): originating dataset
    #   - grain (str): reporting grain description
    #   - note (str): optional extra context
    # Output:
    #   - Dict containing value, display text, status, target, and metadata
    # ================================
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


def compute_inbound_kpis(inbound: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp, sku_families: Optional[Sequence[str]] = None, allowed_parts: Optional[Sequence[str]] = None) -> Dict:
    # ================================
    # Function: compute_inbound_kpis
    # Purpose: Computes inbound supply KPIs for the requested period and scope.
    # Inputs:
    #   - inbound (pd.DataFrame): inbound receipts dataset
    #   - start (pd.Timestamp)
    #   - end (pd.Timestamp)
    #   - sku_families (Optional[Sequence[str]])
    #   - allowed_parts (Optional[Sequence[str]]): part-level scope fallback
    # Output:
    #   - Dict containing the Inbound section and its KPIs
    # ================================
    df = filter_period(inbound, "received_date", start, end)
    df = filter_sku_families(df, sku_families, allowed_parts)
    # Each KPI is computed directly from operational fields so the output stays
    # fully traceable to source tables.
    lead_time = df["inbound_lead_time_days"].mean() if not df.empty else 0.0
    on_time_pct = float((df["received_date"] <= df["expected_date"]).mean()) if not df.empty else 0.0
    discrepancy_pct = safe_divide(df["discrepancy_qty"].sum(), df["qty_ordered"].sum())
    volume = float(df["qty_received"].sum())
    late_count = float((df["received_date"] > df["expected_date"]).sum())
    late_supplier_volume = (
        df.loc[df["received_date"] > df["expected_date"]]
        .groupby("supplier_name", dropna=False)["qty_ordered"]
        .sum()
        .sort_values(ascending=False)
        .head(5)
    )
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
        build_kpi("Average Inbound Lead Time", lead_time, "AVG(inbound_lead_time_days)", "inbound_parts", "reporting period", note=lead_time_comment),
        build_kpi("Receipts On-Time %", on_time_pct, "COUNT(received_date <= expected_date) / COUNT(*)", "inbound_parts", "reporting period", note=on_time_comment),
        build_kpi("Quantity Discrepancy %", discrepancy_pct, "SUM(discrepancy_qty) / SUM(qty_ordered)", "inbound_parts", "reporting period", note=discrepancy_comment),
        build_kpi("Inbound Volume", volume, "SUM(qty_received)", "inbound_parts", "reporting period", note=volume_comment),
        build_kpi("Late Receipt Count", late_count, "COUNT(received_date > expected_date)", "inbound_parts", "reporting period", note=late_count_comment),
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
    return {"name": "Inbound", "kpis": kpis}


def compute_outbound_kpis(outbound: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp, sku_families: Optional[Sequence[str]] = None, allowed_parts: Optional[Sequence[str]] = None) -> Dict:
    # ================================
    # Function: compute_outbound_kpis
    # Purpose: Computes outbound service KPIs such as fill rate and OTIF.
    # Inputs:
    #   - outbound (pd.DataFrame): outbound orders dataset
    #   - start (pd.Timestamp)
    #   - end (pd.Timestamp)
    #   - sku_families (Optional[Sequence[str]])
    #   - allowed_parts (Optional[Sequence[str]]): part-level scope fallback
    # Output:
    #   - Dict containing the Outbound section and its KPIs
    # ================================
    df = filter_period(outbound, "shipped_date", start, end)
    df = filter_sku_families(df, sku_families, allowed_parts)
    fill_rate = safe_divide(df["qty_shipped"].sum(), df["qty_ordered"].sum())
    otif = float(df["otif_flag"].mean()) if not df.empty else 0.0
    backorder_rate = safe_divide(df["backorder_qty"].sum(), df["qty_ordered"].sum())
    volume = float(df["qty_shipped"].sum())
    late_shipments = float((df["shipped_date"] > df["promise_date"]).sum())
    top_backorder_skus = (
        df.groupby("part_number", dropna=False)["backorder_qty"]
        .sum()
        .sort_values(ascending=False)
        .head(10)
    )
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
        build_kpi("Fill Rate %", fill_rate, "SUM(qty_shipped) / SUM(qty_ordered)", "outbound_parts", "reporting period", note=fill_rate_comment),
        build_kpi("OTIF %", otif, "SUM(otif_flag) / COUNT(*)", "outbound_parts", "reporting period", note=otif_comment),
        build_kpi("Backorder Rate %", backorder_rate, "SUM(backorder_qty) / SUM(qty_ordered)", "outbound_parts", "reporting period", note=backorder_comment),
        build_kpi("Outbound Volume", volume, "SUM(qty_shipped)", "outbound_parts", "reporting period", note=volume_comment),
        build_kpi("Late Shipment Count", late_shipments, "COUNT(shipped_date > promise_date)", "outbound_parts", "reporting period", note=late_shipments_comment),
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
    return {"name": "Outbound", "kpis": kpis}


def compute_inventory_kpis(
    inventory: pd.DataFrame,
    outbound: pd.DataFrame,
    start: pd.Timestamp,
    end: pd.Timestamp,
    warehouses: Optional[Sequence[str]] = None,
    sku_families: Optional[Sequence[str]] = None,
    allowed_parts: Optional[Sequence[str]] = None,
) -> Dict:
    # ================================
    # Function: compute_inventory_kpis
    # Purpose: Computes inventory risk and coverage KPIs.
    # Inputs:
    #   - inventory (pd.DataFrame): inventory snapshot dataset
    #   - start (pd.Timestamp)
    #   - end (pd.Timestamp)
    #   - warehouses (Optional[Sequence[str]]): warehouse scope
    #   - sku_families (Optional[Sequence[str]]): SKU-family scope
    # Output:
    #   - Dict containing the Inventory section and its KPIs
    # ================================
    df = filter_period(inventory, "snapshot_date", start, end)
    df = filter_warehouses(df, warehouses)
    df = filter_sku_families(df, sku_families)
    outbound_df = filter_period(outbound, "shipped_date", start, end)
    outbound_df = filter_sku_families(outbound_df, sku_families, allowed_parts)
    dos = df["days_of_supply"].mean() if not df.empty else 0.0
    stockout = float(df["stockout_flag"].mean()) if not df.empty else 0.0
    safety_coverage = float((df["available_qty"] >= df["safety_stock"]).mean()) if not df.empty else 0.0
    aged_pct = float((df["age_days"] > 180).mean()) if not df.empty else 0.0
    avg_age = df["age_days"].mean() if not df.empty else 0.0
    periods_in_month = max(end.days_in_month, 1)
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
        build_kpi("Days of Supply", dos, "AVG(days_of_supply)", "inventory_snapshot", "reporting period", note=dos_comment),
        build_kpi("Stockout Exposure %", stockout, "SUM(stockout_flag) / COUNT(*)", "inventory_snapshot", "reporting period", note=stockout_comment),
        build_kpi("Safety Stock Coverage %", safety_coverage, "COUNT(available_qty >= safety_stock) / COUNT(*)", "inventory_snapshot", "reporting period", note=safety_stock_comment),
        build_kpi("Aged Inventory % (>180d)", aged_pct, "COUNT(age_days > 180) / COUNT(*)", "inventory_snapshot", "reporting period", note=aged_pct_comment),
        build_kpi("Average Inventory Age", avg_age, "AVG(age_days)", "inventory_snapshot", "reporting period", note=avg_age_comment),
        build_kpi(
            "Inventory Turns",
            inventory_turns,
            "ANNUALIZED SUM(qty_shipped) / AVG(on_hand_qty)",
            "inventory_snapshot + outbound_parts",
            "reporting period",
            note=turns_note,
        ),
        build_kpi(
            "Aged Inventory Value >180d",
            aged_inventory_value,
            "SUM(on_hand_qty * unit_cost) WHERE age_days > 180; proxy to SUM(on_hand_qty) when unit_cost is unavailable",
            "inventory_snapshot",
            "reporting period",
            note=aged_value_note,
        ),
    ]
    return {"name": "Inventory", "kpis": kpis}


def compute_warehouse_productivity_kpis(warehouse_df: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp, warehouses: Optional[Sequence[str]] = None) -> Dict:
    # ================================
    # Function: compute_warehouse_productivity_kpis
    # Purpose: Computes warehouse throughput and SLA KPIs.
    # Inputs:
    #   - warehouse_df (pd.DataFrame): warehouse productivity dataset
    #   - start (pd.Timestamp)
    #   - end (pd.Timestamp)
    #   - warehouses (Optional[Sequence[str]]): warehouse scope
    # Output:
    #   - Dict containing the Warehouse Productivity section and its KPIs
    # ================================
    df = filter_period(warehouse_df, "date", start, end)
    df = filter_warehouses(df, warehouses)
    lines_per_hour = safe_divide(df["lines_picked"].sum(), df["labor_hours"].sum())
    orders_per_hour = safe_divide(df["orders_processed"].sum(), df["labor_hours"].sum())
    unique_days = float(df["date"].nunique()) if not df.empty else 0.0
    orders_per_day = safe_divide(df["orders_processed"].sum(), unique_days)
    sla = df["sla_adherence_pct"].mean() if not df.empty else 0.0
    equip_util = df["equipment_utilization_pct"].mean() if not df.empty else 0.0
    touches = df["touches_per_order"].mean() if not df.empty else 0.0
    weakest_shift_row = None
    if not df.empty:
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
        build_kpi("Lines Picked per Labor-Hour", lines_per_hour, "SUM(lines_picked) / SUM(labor_hours)", "warehouse_productivity", "reporting period", note=lines_comment),
        build_kpi("Orders Processed per Labor-Hour", orders_per_hour, "SUM(orders_processed) / SUM(labor_hours)", "warehouse_productivity", "reporting period", note=orders_per_hour_comment),
        build_kpi("Orders per Day", orders_per_day, "SUM(orders_processed) / COUNT(DISTINCT date)", "warehouse_productivity", "reporting period", note=orders_per_day_comment),
        build_kpi("SLA Adherence %", sla, "AVG(sla_adherence_pct)", "warehouse_productivity", "reporting period", note=sla_comment),
        build_kpi("Equipment Utilization %", equip_util, "AVG(equipment_utilization_pct)", "warehouse_productivity", "reporting period", note=equipment_comment),
        build_kpi("Touches per Order", touches, "AVG(touches_per_order)", "warehouse_productivity", "reporting period", note=touches_comment),
    ]
    return {"name": "Warehouse Productivity", "kpis": kpis}


def compute_employee_productivity_kpis(employee_df: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp, warehouses: Optional[Sequence[str]] = None) -> Dict:
    # ================================
    # Function: compute_employee_productivity_kpis
    # Purpose: Computes workforce productivity, quality, and overtime KPIs.
    # Inputs:
    #   - employee_df (pd.DataFrame): employee productivity dataset
    #   - start (pd.Timestamp)
    #   - end (pd.Timestamp)
    #   - warehouses (Optional[Sequence[str]]): warehouse scope
    # Output:
    #   - Dict containing the Employee Productivity section and its KPIs
    # ================================
    df = filter_period(employee_df, "date", start, end)
    df = filter_warehouses(df, warehouses)
    picks_per_hour = safe_divide(df["picks"].sum(), df["hours_worked"].sum())
    error_rate = safe_divide(df["errors"].sum(), df["tasks_completed"].sum())
    rework_rate = safe_divide(df["rework"].sum(), df["tasks_completed"].sum())
    overtime = safe_divide(df["overtime_hours"].sum(), (df["hours_worked"] + df["overtime_hours"]).sum())
    avg_tasks = df["tasks_completed"].mean() if not df.empty else 0.0
    top_error_row = None
    if not df.empty:
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
        build_kpi("Picks per Person per Hour", picks_per_hour, "SUM(picks) / SUM(hours_worked)", "employee_productivity", "reporting period", note=picks_comment),
        build_kpi("Error Rate %", error_rate, "SUM(errors) / SUM(tasks_completed)", "employee_productivity", "reporting period", note=error_comment),
        build_kpi("Rework Rate %", rework_rate, "SUM(rework) / SUM(tasks_completed)", "employee_productivity", "reporting period", note=rework_comment),
        build_kpi("Overtime %", overtime, "SUM(overtime_hours) / SUM(hours_worked + overtime_hours)", "employee_productivity", "reporting period", note=overtime_comment),
        build_kpi("Average Tasks per Employee", avg_tasks, "AVG(tasks_completed)", "employee_productivity", "reporting period", note=avg_tasks_comment),
    ]
    return {"name": "Employee Productivity", "kpis": kpis}


def compute_all_kpis(
    datasets: Dict[str, pd.DataFrame],
    start: pd.Timestamp,
    end: pd.Timestamp,
    warehouses: Optional[Sequence[str]] = None,
    sku_families: Optional[Sequence[str]] = None,
) -> Tuple[List[Dict], List[Dict]]:
    # ================================
    # Function: compute_all_kpis
    # Purpose: Computes every KPI section and flattens the result into a table.
    # Inputs:
    #   - datasets (Dict[str, pd.DataFrame]): all loaded source datasets
    #   - start (pd.Timestamp)
    #   - end (pd.Timestamp)
    #   - warehouses (Optional[Sequence[str]]): warehouse scope
    #   - sku_families (Optional[Sequence[str]]): SKU-family scope
    # Output:
    #   - Tuple[List[Dict], List[Dict]] of section results and flat KPI table
    # Important Logic:
    #   - Builds a part-number scope from inventory when SKU filtering must be
    #     propagated into source tables that do not store `sku_family`
    # ================================
    allowed_parts = None
    if sku_families:
        inventory = datasets["inventory_snapshot"]
        # Inventory is the authoritative source for part-to-family mapping.
        allowed_parts = sorted(
            inventory[inventory["sku_family"].isin(sku_families)]["part_number"].dropna().astype(str).unique().tolist()
        )
    sections = [
        compute_inbound_kpis(datasets["inbound_parts"], start, end, sku_families, allowed_parts),
        compute_outbound_kpis(datasets["outbound_parts"], start, end, sku_families, allowed_parts),
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
    # ================================
    # Function: get_kpi_lookup
    # Purpose: Creates a flat KPI-name lookup for quick access.
    # Inputs:
    #   - sections (List[Dict]): KPI sections
    # Output:
    #   - Dict[str, Dict] keyed by KPI name
    # ================================
    lookup = {}
    for section in sections:
        for kpi in section["kpis"]:
            lookup[kpi["name"]] = kpi
    return lookup
