"""
Deterministic narrative generation for KPI sections and executive actions.

This file sits in the summarization layer of the project. It converts computed
KPI statuses into section risk levels, short section insights, and a concise set
of leadership insights and recommendations without using an LLM.
"""

from __future__ import annotations

from typing import Dict, List, Tuple


def _section_risk_level(section: Dict) -> str:
    # ================================
    # Function: _section_risk_level
    # Purpose: Derives an overall section risk from its KPI statuses.
    # Inputs:
    #   - section (Dict): section containing KPI status values
    # Output:
    #   - str risk level: red, amber, or green
    # ================================
    statuses = [k["status"] for k in section["kpis"] if k["status"] != "info"]
    if "red" in statuses:
        return "red"
    if "amber" in statuses:
        return "amber"
    return "green"


def _first_matching(kpis: Dict[str, Dict], name: str) -> Dict:
    # ================================
    # Function: _first_matching
    # Purpose: Returns a KPI if present, otherwise a safe default placeholder.
    # Inputs:
    #   - kpis (Dict[str, Dict]): flat KPI lookup
    #   - name (str): KPI name to retrieve
    # Output:
    #   - Dict containing the KPI or a default info-only stub
    # ================================
    return kpis.get(name, {"value": 0.0, "display_value": "0", "status": "info"})


def _kpi_note(kpis: Dict[str, Dict], name: str, fallback: str) -> str:
    # Returns the computed KPI note when available, otherwise a fallback.
    note = str(kpis.get(name, {}).get("note", "")).strip()
    return note or fallback


def summarize_sections(sections: List[Dict]) -> List[Dict]:
    # ================================
    # Function: summarize_sections
    # Purpose: Adds deterministic risk labels and section insights.
    # Inputs:
    #   - sections (List[Dict]): computed KPI sections
    # Output:
    #   - List[Dict] with `risk_level` and `insight` added per section
    # Important Logic:
    #   - Uses simple business rules so the summary remains reproducible
    # ================================
    flat = {kpi["name"]: kpi for section in sections for kpi in section["kpis"]}

    inbound_ot = _first_matching(flat, "Receipts On-Time %")
    outbound_otif = _first_matching(flat, "OTIF %")
    stockout = _first_matching(flat, "Stockout Exposure %")
    backorder = _first_matching(flat, "Backorder Rate %")
    lines_per_hour = _first_matching(flat, "Lines Picked per Labor-Hour")
    orders_per_hour = _first_matching(flat, "Orders Processed per Labor-Hour")
    sla = _first_matching(flat, "SLA Adherence %")
    equip = _first_matching(flat, "Equipment Utilization %")
    overtime = _first_matching(flat, "Overtime %")
    errors = _first_matching(flat, "Error Rate %")

    inbound_note = _kpi_note(flat, "Receipts On-Time %", "Inbound service is stable.")
    outbound_note = _kpi_note(flat, "Fill Rate %", _kpi_note(flat, "OTIF %", "Outbound service is healthy."))
    inventory_note = (
        _kpi_note(flat, "Stockout Exposure %", "Inventory coverage is balanced.")
        if stockout["status"] in {"amber", "red"}
        else _kpi_note(flat, "Days of Supply", "Inventory coverage is balanced.")
    )
    warehouse_note = _kpi_note(flat, "Lines Picked per Labor-Hour", _kpi_note(flat, "Equipment Utilization %", "Warehouse throughput is broadly healthy."))
    employee_note = _kpi_note(flat, "Error Rate %", _kpi_note(flat, "Overtime %", "Workforce productivity and quality are balanced."))

    insights_by_section = {
        "Inbound": (
            "Inbound service is stable."
            if inbound_ot["status"] == "green"
            else f"{inbound_note}; engage supplier recovery on delayed receipts."
        ),
        "Outbound": (
            "Outbound service is healthy."
            if outbound_otif["status"] == "green" and backorder["status"] == "green"
            else f"{outbound_note}; review fulfillment and replenishment priorities."
        ),
        "Inventory": (
            "Inventory coverage is balanced."
            if stockout["status"] == "green"
            else f"{inventory_note}; trigger targeted expedite or stock rebalance."
        ),
        "Warehouse Productivity": (
            "Warehouse throughput is broadly healthy."
            if lines_per_hour["status"] == "green" and orders_per_hour["status"] == "green" and sla["status"] == "green"
            else (
                f"{warehouse_note}; rebalance labor or maintenance windows."
                if any(kpi["status"] in {"amber", "red"} for kpi in [lines_per_hour, orders_per_hour, sla, equip])
                else f"Warehouse operations need attention, with equipment utilization at {equip['display_value']}."
            )
        ),
        "Employee Productivity": (
            "Workforce productivity and quality are balanced."
            if overtime["status"] == "green" and errors["status"] == "green"
            else f"{employee_note}; add shift-level coaching or staffing support."
        ),
    }

    for section in sections:
        section["risk_level"] = _section_risk_level(section)
        section["insight"] = insights_by_section.get(section["name"], "No additional insight available.")
    return sections


def build_insights_risks_and_recommendations(sections: List[Dict]) -> Tuple[List[str], List[str], List[str]]:
    # ================================
    # Function: build_insights_and_recommendations
    # Purpose: Produces cross-domain leadership insights and next actions.
    # Inputs:
    #   - sections (List[Dict]): computed KPI sections with statuses
    # Output:
    #   - Tuple[List[str], List[str], List[str]] of insights, risks, and recommendations
    # Important Logic:
    #   - Connects related KPI signals across supply, inventory, warehouse, and
    #     workforce performance to generate actionable business narrative
    # ================================
    kpis = {kpi["name"]: kpi for section in sections for kpi in section["kpis"]}
    insights: List[str] = []
    risks: List[str] = []
    recommendations: List[str] = []

    inbound_note = _kpi_note(kpis, "Receipts On-Time %", "Inbound performance is stable")
    outbound_note = _kpi_note(kpis, "Fill Rate %", _kpi_note(kpis, "OTIF %", "Outbound service is stable"))
    inventory_note = (
        _kpi_note(kpis, "Stockout Exposure %", "Inventory coverage is stable")
        if kpis["Stockout Exposure %"]["status"] in {"amber", "red"} or kpis["Safety Stock Coverage %"]["status"] in {"amber", "red"}
        else _kpi_note(kpis, "Days of Supply", "Inventory coverage is stable")
    )
    warehouse_note = _kpi_note(kpis, "Lines Picked per Labor-Hour", _kpi_note(kpis, "Equipment Utilization %", "Warehouse productivity is stable"))
    employee_note = _kpi_note(kpis, "Error Rate %", _kpi_note(kpis, "Overtime %", "Employee productivity is stable"))

    if kpis["Receipts On-Time %"]["status"] in {"amber", "red"}:
        insights.append(f"Inbound: {inbound_note}; engage supplier capacity or recovery planning.")
        risks.append("Late inbound receipts are increasing the risk of continued outbound service misses.")
        recommendations.append("Escalate late suppliers and expedite open receipt lines tied to service-risk parts.")

    if kpis["Backorder Rate %"]["status"] in {"amber", "red"} or kpis["OTIF %"]["status"] in {"amber", "red"}:
        insights.append(f"Outbound: {outbound_note}; review safety stock and order-priority settings.")
        risks.append("Elevated backorders create immediate customer-service and fulfillment risk.")
        recommendations.append("Prioritize the SKUs driving backorders and clear the most delayed customer orders first.")

    if kpis["Stockout Exposure %"]["status"] in {"amber", "red"} or kpis["Safety Stock Coverage %"]["status"] in {"amber", "red"}:
        insights.append(f"Inventory: {inventory_note}; trigger expedite or rebalance inventory.")
        risks.append("Inventory coverage gaps may continue to drive stockouts and service degradation.")
        recommendations.append("Recalibrate reorder points and safety stock for the highest-risk SKUs and constrained warehouses.")

    if any(kpis[name]["status"] in {"amber", "red"} for name in ["Lines Picked per Labor-Hour", "Orders Processed per Labor-Hour", "SLA Adherence %", "Equipment Utilization %"]):
        insights.append(f"Warehouse Productivity: {warehouse_note}; rebalance labor or maintenance windows.")
        recommendations.append("Address the weakest warehouse throughput driver before adding broader process redesign.")

    if kpis["Overtime %"]["status"] in {"amber", "red"} or kpis["Error Rate %"]["status"] in {"amber", "red"}:
        insights.append(f"Employee: {employee_note}; add refresher training or staffing support.")
        risks.append("Workforce strain may reduce quality and worsen operational stability if overtime remains elevated.")
        recommendations.append("Target the highest-error shift or employee cluster with training and overtime relief.")

    if not insights:
        insights.append("Overall performance is broadly stable, with no major domain simultaneously failing across service, inventory, and workforce metrics.")
    if not risks:
        risks.append("No acute cross-domain operational risk is dominant in the current reporting period.")
    if not recommendations:
        recommendations.append("Continue monthly KPI review and extend the solution with prior-period trend comparison for earlier risk detection.")

    return insights[:5], risks[:3], recommendations[:5]
