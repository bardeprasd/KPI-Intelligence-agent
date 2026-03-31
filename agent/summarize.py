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

    insights_by_section = {
        "Inbound": (
            "Inbound service is stable."
            if inbound_ot["status"] == "green"
            else f"Inbound timeliness is under target at {inbound_ot['display_value']}, increasing supplier receipt risk."
        ),
        "Outbound": (
            "Outbound service is healthy."
            if outbound_otif["status"] == "green" and backorder["status"] == "green"
            else f"Outbound service is pressured by OTIF at {outbound_otif['display_value']} and backorder rate at {backorder['display_value']}."
        ),
        "Inventory": (
            "Inventory coverage is balanced."
            if stockout["status"] == "green"
            else f"Inventory risk is elevated with stockout exposure at {stockout['display_value']}."
        ),
        "Warehouse Productivity": (
            "Warehouse throughput is broadly healthy."
            if lines_per_hour["status"] == "green" and orders_per_hour["status"] == "green" and sla["status"] == "green"
            else (
                f"Warehouse throughput is under target, with lines per labor-hour at {lines_per_hour['display_value']}, "
                f"orders per labor-hour at {orders_per_hour['display_value']}, and SLA adherence at {sla['display_value']}."
                if any(kpi["status"] in {"amber", "red"} for kpi in [lines_per_hour, orders_per_hour, sla])
                else f"Warehouse operations need attention, with equipment utilization at {equip['display_value']}."
            )
        ),
        "Employee Productivity": (
            "Workforce productivity and quality are balanced."
            if overtime["status"] == "green" and errors["status"] == "green"
            else f"Workforce pressure is visible, with overtime at {overtime['display_value']} and error rate at {errors['display_value']}."
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

    if kpis["Receipts On-Time %"]["status"] in {"amber", "red"} and kpis["OTIF %"]["status"] in {"amber", "red"}:
        insights.append("Supply reliability is likely contributing to outbound service pressure because inbound timeliness and OTIF are both below target.")
        risks.append("Late inbound receipts are increasing the risk of continued outbound service misses.")
        recommendations.append("Review late inbound receipts by supplier and expedite constrained part numbers that also appear in service-risk outbound orders.")

    if kpis["Stockout Exposure %"]["status"] in {"amber", "red"} or kpis["Safety Stock Coverage %"]["status"] in {"amber", "red"}:
        insights.append("Inventory resilience is weak, with stockout exposure and/or safety stock coverage outside target levels.")
        risks.append("Inventory coverage gaps may continue to drive stockouts and service degradation.")
        recommendations.append("Recalibrate reorder points and safety stock for SKUs driving stockout exposure, especially in the most constrained warehouses.")

    if kpis["Backorder Rate %"]["status"] in {"amber", "red"}:
        insights.append("Backorders are materially affecting service performance and should be treated as a customer-impacting issue.")
        risks.append("Elevated backorders create immediate customer-service and fulfillment risk.")
        recommendations.append("Prioritize root-cause analysis on backordered parts and align fulfillment, planning, and supplier recovery actions.")

    if kpis["Overtime %"]["status"] in {"amber", "red"} and kpis["Error Rate %"]["status"] in {"amber", "red"}:
        insights.append("Elevated overtime together with quality errors suggests workforce strain may be increasing operational risk.")
        risks.append("Workforce strain may reduce quality and worsen operational stability if overtime remains elevated.")
        recommendations.append("Rebalance shift staffing, review training needs, and monitor whether overtime reduction improves quality outcomes.")

    if kpis["Lines Picked per Labor-Hour"]["status"] == "green" and kpis["OTIF %"]["status"] in {"amber", "red"}:
        insights.append("Warehouse throughput is relatively stable, so service issues are more likely caused by supply or inventory constraints than floor productivity.")
        recommendations.append("Focus corrective actions on supply availability and inventory positioning before redesigning warehouse labor processes.")

    if not insights:
        insights.append("Overall performance is broadly stable, with no major domain simultaneously failing across service, inventory, and workforce metrics.")
    if not risks:
        risks.append("No acute cross-domain operational risk is dominant in the current reporting period.")
    if not recommendations:
        recommendations.append("Continue monthly KPI review and extend the solution with prior-period trend comparison for earlier risk detection.")

    return insights[:5], risks[:3], recommendations[:5]
