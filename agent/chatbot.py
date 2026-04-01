"""
Grounded KPI chatbot for interactive question answering.

This file sits in the chatbot/application layer of the project. It loads the
same KPI payload produced by the core pipeline, tracks lightweight conversation
memory, answers common questions deterministically, and can optionally call
OpenAI for grounded natural-language responses.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence, Tuple

import pandas as pd

from .ingest import derive_default_period, load_all
from .kpi import compute_all_kpis
from .config import PathsConfig
from .summarize import build_insights_risks_and_recommendations, summarize_sections


CHATBOT_SYSTEM_PROMPT = """You are a KPI intelligence chatbot for a warehouse and supply-chain leadership assignment.
You must answer only from the grounded KPI payload, warehouse scope, reporting period, and provided conversation memory.
Do not invent facts, causes, trends, warehouse codes, or KPI values.
If the answer is not supported by the provided data, say so clearly.
Keep answers concise, business-friendly, and helpful.
Use conversation memory when the user asks follow-up questions.
If a question asks for KPI values or comparisons, answer using the grounded payload rather than generic advice.
"""


def _compact_kpi_for_llm(kpi: Dict) -> Dict:
    return {
        "name": kpi.get("name"),
        "display_value": kpi.get("display_value"),
        "value": kpi.get("value"),
        "unit": kpi.get("unit"),
        "status": kpi.get("status"),
        "target_display": kpi.get("target_display"),
        "note": kpi.get("note"),
    }


def _compact_section_for_llm(section: Dict) -> Dict:
    return {
        "name": section.get("name"),
        "risk_level": section.get("risk_level"),
        "insight": section.get("insight"),
        "kpis": [_compact_kpi_for_llm(kpi) for kpi in section.get("kpis", [])],
    }


def _compact_summary_card_for_llm(card: Dict) -> Dict:
    return {
        "label": card.get("label"),
        "display_value": card.get("display_value"),
        "status": card.get("status"),
        "target_display": card.get("target_display"),
        "note": card.get("note"),
    }
# Alias maps translate natural user phrasing into canonical KPI names.
KPI_ALIASES = {
    "fill rate": "Fill Rate %",
    "otif": "OTIF %",
    "on time in full": "OTIF %",
    "backorder": "Backorder Rate %",
    "backorder rate": "Backorder Rate %",
    "days of supply": "Days of Supply",
    "dos": "Days of Supply",
    "stockout": "Stockout Exposure %",
    "stockout exposure": "Stockout Exposure %",
    "safety stock": "Safety Stock Coverage %",
    "aged inventory": "Aged Inventory % (>180d)",
    "inventory age": "Average Inventory Age",
    "lead time": "Average Inbound Lead Time",
    "inbound lead time": "Average Inbound Lead Time",
    "receipts on time": "Receipts On-Time %",
    "discrepancy": "Quantity Discrepancy %",
    "inbound volume": "Inbound Volume",
    "late receipts": "Late Receipt Count",
    "outbound volume": "Outbound Volume",
    "late shipments": "Late Shipment Count",
    "lines picked": "Lines Picked per Labor-Hour",
    "lines picked per labor-hour": "Lines Picked per Labor-Hour",
    "orders processed": "Orders Processed per Labor-Hour",
    "sla": "SLA Adherence %",
    "equipment utilization": "Equipment Utilization %",
    "touches per order": "Touches per Order",
    "picks per person per hour": "Picks per Person per Hour",
    "picks per hour": "Picks per Person per Hour",
    "error rate": "Error Rate %",
    "rework rate": "Rework Rate %",
    "overtime": "Overtime %",
    "average tasks": "Average Tasks per Employee",
}
# Domain aliases support broad section-level questions such as "show inventory".
DOMAIN_ALIASES = {
    "inbound": "Inbound",
    "outbound": "Outbound",
    "inventory": "Inventory",
    "warehouse": "Warehouse Productivity",
    "warehouse productivity": "Warehouse Productivity",
    "employee": "Employee Productivity",
    "employee productivity": "Employee Productivity",
}
DRILLDOWN_ALIASES = {
    "warehouse": "by_warehouse",
    "warehouses": "by_warehouse",
    "sku family": "by_sku_family",
    "sku families": "by_sku_family",
    "part number": "by_part_number",
    "part": "by_part_number",
    "sku": "by_part_number",
    "employee": "by_employee",
    "supplier": "by_supplier",
    "customer": "by_customer",
    "date": "by_date",
    "day": "by_date",
    "week": "by_date",
    "month": "by_date",
}
TIME_GRAIN_ALIASES = {
    "day": "day",
    "daily": "day",
    "week": "week",
    "weekly": "week",
    "month": "month",
    "monthly": "month",
}
# Only these KPIs can be recomputed at warehouse scope from the source data.
WAREHOUSE_AWARE_KPIS = {
    "Days of Supply",
    "Stockout Exposure %",
    "Safety Stock Coverage %",
    "Aged Inventory % (>180d)",
    "Average Inventory Age",
    "Lines Picked per Labor-Hour",
    "Orders Processed per Labor-Hour",
    "SLA Adherence %",
    "Equipment Utilization %",
    "Touches per Order",
    "Picks per Person per Hour",
    "Error Rate %",
    "Rework Rate %",
    "Overtime %",
    "Average Tasks per Employee",
}


@dataclass
class ChatMemory:
    # ================================
    # Class: ChatMemory
    # Purpose: Stores minimal conversation context for follow-up questions.
    # Inputs:
    #   - Optional last KPI, domain, warehouse list, and prior turns
    # Output:
    #   - Mutable memory object owned by the chatbot instance
    # ================================
    last_kpi: Optional[str] = None
    last_domain: Optional[str] = None
    last_warehouses: List[str] = field(default_factory=list)
    turns: List[Tuple[str, str]] = field(default_factory=list)


class KPIChatbot:
    def __init__(self, *, datasets: Dict[str, pd.DataFrame], start: pd.Timestamp, end: pd.Timestamp, payload: Dict, warehouse_scope: Optional[Sequence[str]] = None) -> None:
        # ================================
        # Function: __init__
        # Purpose: Initializes a chatbot with already-loaded data and KPI payload.
        # Inputs:
        #   - datasets (Dict[str, pd.DataFrame]): source datasets
        #   - start (pd.Timestamp): report start date
        #   - end (pd.Timestamp): report end date
        #   - payload (Dict): packaged KPI payload for grounded responses
        #   - warehouse_scope (Optional[Sequence[str]]): active warehouse filter
        # Output:
        #   - None
        # ================================
        self.datasets = datasets
        self.start = start
        self.end = end
        self.payload = payload
        self.warehouse_scope = list(warehouse_scope or [])
        self.memory = ChatMemory(last_warehouses=list(warehouse_scope or []))
        self.sections = {section["name"]: section for section in payload["sections"]}
        self.kpi_lookup = {kpi["name"]: kpi for section in payload["sections"] for kpi in section["kpis"]}
        self.kpi_to_section = {kpi["name"]: section["name"] for section in payload["sections"] for kpi in section["kpis"]}
        # Build the list of warehouses once so later comparisons and validation
        # can reuse a consistent set of supported IDs.
        self.available_warehouses = sorted(
            set(datasets["inventory_snapshot"]["warehouse_id"].dropna().astype(str).unique())
            | set(datasets["warehouse_productivity"]["warehouse_id"].dropna().astype(str).unique())
            | set(datasets["employee_productivity"]["warehouse_id"].dropna().astype(str).unique())
        )

    @classmethod
    def from_project(
        cls,
        *,
        start: Optional[str] = None,
        end: Optional[str] = None,
        warehouses: Optional[Sequence[str]] = None,
        sku_families: Optional[Sequence[str]] = None,
        use_llm_summary: bool = False,
        llm_model: str = "gpt-4.1-mini",
    ) -> "KPIChatbot":
        # ================================
        # Function: from_project
        # Purpose: Factory that builds a chatbot directly from project inputs.
        # Inputs:
        #   - start, end, warehouses, sku_families
        #   - use_llm_summary (bool): whether to polish section narrative
        #   - llm_model (str): model used for optional narrative generation
        # Output:
        #   - KPIChatbot instance ready to answer grounded questions
        # Important Logic:
        #   - Reuses the same ingestion and KPI pipeline as the batch runner
        # ================================
        paths = PathsConfig()
        datasets, row_counts, validation_warnings = load_all(paths.data_dir)
        if start and end:
            start_ts = pd.to_datetime(start)
            end_ts = pd.to_datetime(end)
            label = start_ts.strftime("%B %Y") if start_ts.to_period("M") == end_ts.to_period("M") else f"{start_ts.date()} to {end_ts.date()}"
        else:
            start_ts, end_ts, label = derive_default_period(datasets)

        sections, kpi_table = compute_all_kpis(datasets, start_ts, end_ts, warehouses, sku_families)
        from .output import build_payload

        sections = summarize_sections(sections)
        insights, risks, recommendations = build_insights_risks_and_recommendations(sections)
        llm_used = False
        if use_llm_summary:
            from .llm_summary import LLMPolishError, generate_narrative_with_openai
            # This optional step rewrites narrative text only; KPI values stay
            # grounded in deterministic calculations.
            try:
                sections, insights, recommendations = generate_narrative_with_openai(sections=sections, model=llm_model)
                llm_used = True
            except LLMPolishError:
                pass
        payload = build_payload(
            sections=sections,
            kpi_table=kpi_table,
            insights=insights,
            risks=risks,
            recommendations=recommendations,
            row_counts=row_counts,
            validation_warnings=validation_warnings,
            start_date=start_ts,
            end_date=end_ts,
            period_label=label,
            warehouse_filter=list(warehouses or []),
            sku_family_filter=list(sku_families or []),
            llm_used=llm_used,
        )
        return cls(datasets=datasets, start=start_ts, end=end_ts, payload=payload, warehouse_scope=warehouses)

    def answer(self, question: str) -> str:
        # ================================
        # Function: answer
        # Purpose: Answers a user question using deterministic rules and memory.
        # Inputs:
        #   - question (str): user question in natural language
        # Output:
        #   - str chatbot answer grounded in the KPI payload
        # Important Logic:
        #   - Detects KPI names, domains, and warehouse IDs from free text
        #   - Reuses conversation memory for follow-up questions
        #   - Falls back to guided examples when intent is unclear
        # ================================
        question = question.strip()
        if not question:
            return "Ask about KPIs, risks, recommendations, a warehouse comparison, or the reporting period."

        lower = question.lower()
        warehouses = self._extract_warehouses(lower)
        domain = self._detect_domain(lower)
        kpi = self._detect_kpi(lower)
        drilldown = self._detect_drilldown(lower)
        time_grain = self._detect_time_grain(lower)

        if lower in {"help", "menu", "what can you do?", "what can you do", "examples"}:
            return self._help_text()
        if any(term in lower for term in ["reporting period", "which month", "what month", "period"]) and "compare" not in lower:
            return self._reporting_period_text()
        if any(term in lower for term in ["overall", "executive summary", "top summary", "summary"]) and not kpi and not domain:
            return self._executive_summary_text()
        if "risk" in lower:
            return self._risk_text()
        if any(term in lower for term in ["recommendation", "action", "next step"]):
            return self._recommendation_text()
        if any(term in lower for term in ["kpis", "available metrics", "list metrics"]):
            return self._list_kpis_text(domain)

        drilldown_intent = (" by " in lower) or ("drill" in lower) or any(
            term in lower for term in ["which warehouse", "which sku family", "which part", "which employee", "causing", "driver"]
        )
        if drilldown and drilldown_intent and (kpi or domain):
            response = self._drilldown_response(
                kpi=kpi,
                domain=domain,
                drilldown_id=drilldown,
                warehouses=warehouses,
                time_grain=time_grain,
                lower_question=lower,
            )
            self._remember(question, response, kpi=kpi, domain=domain, warehouses=warehouses)
            return response

        # Follow-up prompts such as "How about WH-03?" inherit the last KPI when
        # the current question only changes the warehouse context.
        if not kpi and warehouses and self.memory.last_kpi and any(term in lower for term in ["how about", "and", "what about"]):
            kpi = self.memory.last_kpi
        if not kpi and "compare" in lower and self.memory.last_kpi:
            kpi = self.memory.last_kpi
            if not warehouses and len(self.memory.last_warehouses) >= 2:
                warehouses = self.memory.last_warehouses[:2]

        if "compare" in lower and kpi:
            warehouses = warehouses or self.memory.last_warehouses or self.available_warehouses[:2]
            response = self._compare_kpi(kpi, warehouses)
            self._remember(question, response, kpi=kpi, warehouses=warehouses, domain=domain)
            return response

        if any(term in lower for term in ["highest", "lowest", "best", "worst"]) and kpi:
            response = self._rank_warehouses(kpi, lower)
            self._remember(question, response, kpi=kpi, domain=domain)
            return response

        if domain and not kpi:
            response = self._domain_summary(domain, warehouses)
            self._remember(question, response, domain=domain, warehouses=warehouses)
            return response

        if kpi:
            response = self._kpi_response(kpi, warehouses)
            self._remember(question, response, kpi=kpi, domain=domain, warehouses=warehouses)
            return response

        fallback = (
            "I can answer questions like: 'What is OTIF?', 'Show inventory KPIs', "
            "'Which warehouse has the highest error rate?', 'Compare WH-01 and WH-02 on SLA', "
            "or 'What are the top recommendations?'."
        )
        self._remember(question, fallback)
        return fallback

    def answer_with_openai(self, question: str, *, model: str = "gpt-4.1-mini") -> str:
        # ================================
        # Function: answer_with_openai
        # Purpose: Uses OpenAI to answer a question from grounded KPI context.
        # Inputs:
        #   - question (str): user question
        #   - model (str): OpenAI model name
        # Output:
        #   - str model-generated answer
        # Important Logic:
        #   - Sends only structured KPI payload plus recent memory
        #   - Rejects empty responses and fails clearly when OpenAI is unavailable
        # ================================
        if not os.getenv("OPENAI_API_KEY"):
            raise RuntimeError("OPENAI_API_KEY is not set.")
        try:
            from openai import OpenAI
        except Exception as exc:  # pragma: no cover
            raise RuntimeError("OpenAI package is not installed. Install with: pip install openai") from exc

        client = OpenAI()
        # Keep the model context compact and factual so answers remain anchored
        # to the same payload shown elsewhere in the application.
        grounded_payload = {
            "reporting_period": self.payload.get("reporting_period"),
            "header": {
                "title": self.payload.get("header", {}).get("title"),
                "overall_status": self.payload.get("header", {}).get("overall_status"),
                "warehouse_scope": self.payload.get("header", {}).get("warehouse_scope"),
                "sku_family_scope": self.payload.get("header", {}).get("sku_family_scope"),
            },
            "summary_cards": [_compact_summary_card_for_llm(card) for card in self.payload.get("summary_cards", [])],
            "sections": [_compact_section_for_llm(section) for section in self.payload.get("sections", [])],
            "insights": self.payload.get("insights", [])[:5],
            "recommendations": self.payload.get("recommendations", [])[:5],
            "available_warehouses": self.available_warehouses,
            "warehouse_scope": self.warehouse_scope,
        }
        # Only pass a small rolling window of recent turns to keep follow-up
        # context without overloading the prompt.
        memory = [
            {"user": q, "assistant": a}
            for q, a in self.memory.turns[-3:]
        ]
        try:
            response = client.responses.create(
                model=model,
                input=[
                    {"role": "system", "content": CHATBOT_SYSTEM_PROMPT},
                    {
                        "role": "user",
                        "content": json.dumps(
                            {
                                "question": question,
                                "conversation_memory": memory,
                                "grounded_kpi_payload": grounded_payload,
                            },
                            ensure_ascii=False,
                        ),
                    },
                ],
                temperature=0.2,
            )
        except Exception as exc:
            raise RuntimeError(f"OpenAI chatbot request failed: {exc}") from exc
        answer = (getattr(response, "output_text", "") or "").strip()
        if not answer:
            raise RuntimeError("OpenAI chatbot response was empty.")
        self._remember(question, answer)
        return answer

    def answer_auto(self, question: str, *, use_openai: bool = False, model: str = "gpt-4.1-mini") -> str:
        # ================================
        # Function: answer_auto
        # Purpose: Routes a question to deterministic or OpenAI-backed answering.
        # Inputs:
        #   - question (str): user question
        #   - use_openai (bool): whether to use the OpenAI answer path
        #   - model (str): model name for the OpenAI path
        # Output:
        #   - str chatbot answer
        # ================================
        if use_openai:
            try:
                return self.answer_with_openai(question, model=model)
            except RuntimeError as exc:
                print(f"LLM chatbot unavailable. Falling back to deterministic mode. Reason: {exc}")
                return self.answer(question)
        return self.answer(question)

    def _remember(self, question: str, answer: str, *, kpi: Optional[str] = None, domain: Optional[str] = None, warehouses: Optional[Sequence[str]] = None) -> None:
        # ================================
        # Function: _remember
        # Purpose: Updates the lightweight conversation memory after each turn.
        # Inputs:
        #   - question (str)
        #   - answer (str)
        #   - kpi (Optional[str])
        #   - domain (Optional[str])
        #   - warehouses (Optional[Sequence[str]])
        # Output:
        #   - None
        # ================================
        if kpi:
            self.memory.last_kpi = kpi
        if domain:
            self.memory.last_domain = domain
        if warehouses:
            self.memory.last_warehouses = list(warehouses)
        self.memory.turns.append((question, answer))

    def _help_text(self) -> str:
        # ================================
        # Function: _help_text
        # Purpose: Returns example questions for the user.
        # Inputs:
        #   - None
        # Output:
        #   - str help text
        # ================================
        examples = [
            "What is the fill rate?",
            "Show the inventory section.",
            "Which warehouse has the highest error rate?",
            "Compare WH-01 and WH-02 on SLA.",
            "What are the top risks?",
            "How about WH-03?",
        ]
        return "Try one of these questions:\n- " + "\n- ".join(examples)

    def _reporting_period_text(self) -> str:
        # ================================
        # Function: _reporting_period_text
        # Purpose: Describes the active reporting period and warehouse scope.
        # Inputs:
        #   - None
        # Output:
        #   - str period summary
        # ================================
        scope = ", ".join(self.warehouse_scope) if self.warehouse_scope else "all warehouses / full network"
        return f"The current reporting period is {self.payload['reporting_period']['label']} ({self.start.date()} to {self.end.date()}) with scope: {scope}."

    def _executive_summary_text(self) -> str:
        # ================================
        # Function: _executive_summary_text
        # Purpose: Summarizes the top KPI cards and overall status.
        # Inputs:
        #   - None
        # Output:
        #   - str executive summary
        # ================================
        cards = "; ".join(f"{card['label']}: {card['display_value']} ({card['status']})" for card in self.payload["summary_cards"])
        return f"Overall status is {self.payload['header']['overall_status'].upper()}. Summary cards: {cards}."

    def _risk_text(self) -> str:
        # ================================
        # Function: _risk_text
        # Purpose: Returns section-level risk posture with supporting insights.
        # Inputs:
        #   - None
        # Output:
        #   - str risk summary
        # ================================
        section_risks = [f"{section['name']}: {section.get('risk_level', 'unknown')} risk" for section in self.payload['sections']]
        insights = "; ".join(self.payload.get("insights", [])[:3])
        return f"Top risks by section: {'; '.join(section_risks)}. Key supporting insights: {insights}."

    def _recommendation_text(self) -> str:
        # ================================
        # Function: _recommendation_text
        # Purpose: Returns top recommended actions from the payload.
        # Inputs:
        #   - None
        # Output:
        #   - str recommendation summary
        # ================================
        recs = self.payload.get("recommendations", [])[:5]
        return "Recommended actions: " + " | ".join(recs)

    def _list_kpis_text(self, domain: Optional[str] = None) -> str:
        # ================================
        # Function: _list_kpis_text
        # Purpose: Lists KPI names globally or for a specific domain.
        # Inputs:
        #   - domain (Optional[str]): requested section name
        # Output:
        #   - str list of KPIs
        # ================================
        if domain and domain in self.sections:
            kpis = [kpi["name"] for kpi in self.sections[domain]["kpis"]]
            return f"Available KPIs for {domain}: " + ", ".join(kpis)
        return "Available KPI sections: " + "; ".join(
            f"{name}: {', '.join(k['name'] for k in section['kpis'])}" for name, section in self.sections.items()
        )

    def _domain_summary(self, domain: str, warehouses: Sequence[str]) -> str:
        # ================================
        # Function: _domain_summary
        # Purpose: Summarizes all KPIs for a section, optionally by warehouse.
        # Inputs:
        #   - domain (str): canonical section name
        #   - warehouses (Sequence[str]): requested warehouse scope
        # Output:
        #   - str section summary
        # ================================
        if warehouses:
            section = self._section_for_warehouses(domain, warehouses)
            if not section:
                return f"{domain} does not support warehouse-specific slicing in the provided data."
            kpi_text = "; ".join(f"{k['name']}: {k['display_value']} ({k['status']})" for k in section['kpis'])
            return f"{domain} for {', '.join(warehouses)}: {kpi_text}."
        section = self.sections[domain]
        kpi_text = "; ".join(f"{k['name']}: {k['display_value']} ({k['status']})" for k in section['kpis'])
        return f"{domain}: {kpi_text}. Insight: {section.get('insight', 'No additional insight generated.')}"

    def _kpi_response(self, kpi: str, warehouses: Sequence[str]) -> str:
        # ================================
        # Function: _kpi_response
        # Purpose: Returns a KPI answer for network or warehouse scope.
        # Inputs:
        #   - kpi (str): canonical KPI name
        #   - warehouses (Sequence[str]): optional warehouse scope
        # Output:
        #   - str KPI response
        # ================================
        if warehouses:
            if len(warehouses) == 1:
                value = self._warehouse_kpi_value(kpi, warehouses[0])
                if not value:
                    return f"{kpi} is not available at warehouse level in the provided data. Inbound and outbound KPIs are network-level only."
                return f"{kpi} for {warehouses[0]} in {self.payload['reporting_period']['label']} is {value['display_value']} with status {value['status']}."
            return self._compare_kpi(kpi, warehouses)

        value = self.kpi_lookup[kpi]
        target = f" against target {value['target_display']}" if value.get('target_display') else ""
        return f"{kpi} for {self.payload['reporting_period']['label']} is {value['display_value']} with status {value['status']}{target}."

    def _compare_kpi(self, kpi: str, warehouses: Sequence[str]) -> str:
        # ================================
        # Function: _compare_kpi
        # Purpose: Compares a warehouse-aware KPI across up to three warehouses.
        # Inputs:
        #   - kpi (str): canonical KPI name
        #   - warehouses (Sequence[str]): requested warehouse IDs
        # Output:
        #   - str comparison summary
        # Important Logic:
        #   - Deduplicates warehouses
        #   - Chooses sort direction based on whether higher or lower values are better
        # ================================
        unique_warehouses: List[str] = []
        for w in warehouses:
            if w not in unique_warehouses:
                unique_warehouses.append(w)
        warehouses = unique_warehouses[:3]
        values = []
        for warehouse in warehouses:
            value = self._warehouse_kpi_value(kpi, warehouse)
            if value:
                values.append((warehouse, value))
        if len(values) < 2:
            return f"I could not compare {kpi} across the requested warehouses. This KPI may only exist at network level."

        # Some KPIs are healthy when low, while others improve when high. This
        # flag controls the comparison ranking narrative.
        higher_is_better = not ("Rate %" in kpi and kpi in {"Backorder Rate %", "Error Rate %", "Rework Rate %", "Overtime %", "Stockout Exposure %", "Aged Inventory % (>180d)"}) and kpi not in {"Average Inbound Lead Time", "Touches per Order", "Average Inventory Age", "Days of Supply"}
        sorted_values = sorted(values, key=lambda item: item[1]["value"], reverse=higher_is_better)
        comparison = "; ".join(f"{warehouse}: {value['display_value']} ({value['status']})" for warehouse, value in values)
        best = sorted_values[0][0]
        worst = sorted_values[-1][0]
        return f"{kpi} comparison for {', '.join(warehouses)}: {comparison}. Best performer: {best}. Weakest performer: {worst}."

    def _rank_warehouses(self, kpi: str, lower_question: str) -> str:
        # ================================
        # Function: _rank_warehouses
        # Purpose: Finds the best, worst, highest, or lowest warehouse for a KPI.
        # Inputs:
        #   - kpi (str): canonical KPI name
        #   - lower_question (str): lowercase user question for intent parsing
        # Output:
        #   - str ranking answer
        # ================================
        warehouse_values = []
        for warehouse in self.available_warehouses:
            value = self._warehouse_kpi_value(kpi, warehouse)
            if value:
                warehouse_values.append((warehouse, value))
        if not warehouse_values:
            return f"{kpi} is not warehouse-grained in the current data, so I cannot rank warehouses for it."

        # Ranking direction depends on the business meaning of the KPI.
        lower_is_better = kpi in {"Error Rate %", "Rework Rate %", "Overtime %", "Stockout Exposure %", "Aged Inventory % (>180d)", "Touches per Order", "Average Inventory Age"}
        if kpi == "Days of Supply":
            lower_is_better = False
        if any(term in lower_question for term in ["lowest", "best"]):
            ranked = sorted(warehouse_values, key=lambda item: item[1]["value"], reverse=not lower_is_better)
            label = "best" if "best" in lower_question else "lowest"
        else:
            ranked = sorted(warehouse_values, key=lambda item: item[1]["value"], reverse=not lower_is_better)
            label = "highest" if "highest" in lower_question else "worst"
        winner = ranked[0]
        return f"{label.title()} {kpi} is {winner[0]} at {winner[1]['display_value']} with status {winner[1]['status']}."

    def _warehouse_kpi_value(self, kpi: str, warehouse: str) -> Optional[Dict]:
        # ================================
        # Function: _warehouse_kpi_value
        # Purpose: Recomputes a warehouse-level KPI from source data on demand.
        # Inputs:
        #   - kpi (str): canonical KPI name
        #   - warehouse (str): warehouse ID
        # Output:
        #   - Optional[Dict] with KPI data, or None if not warehouse-aware
        # Important Logic:
        #   - Re-runs KPI computation for the requested warehouse to avoid stale
        #     pre-aggregated values in follow-up chatbot comparisons
        # ================================
        if kpi not in WAREHOUSE_AWARE_KPIS:
            return None
        sections, _ = compute_all_kpis(self.datasets, self.start, self.end, [warehouse])
        lookup = {item["name"]: item for section in sections for item in section["kpis"]}
        return lookup.get(kpi)

    def _section_for_warehouses(self, domain: str, warehouses: Sequence[str]) -> Optional[Dict]:
        # ================================
        # Function: _section_for_warehouses
        # Purpose: Recomputes one section for a warehouse-scoped question.
        # Inputs:
        #   - domain (str): canonical section name
        #   - warehouses (Sequence[str]): warehouse IDs
        # Output:
        #   - Optional[Dict] section payload, or None when unsupported
        # ================================
        temp_sections, _ = compute_all_kpis(self.datasets, self.start, self.end, list(warehouses))
        for section in temp_sections:
            if section["name"] == domain:
                if any(kpi["name"] in WAREHOUSE_AWARE_KPIS for kpi in section["kpis"]):
                    return section
                return None
        return None

    def _extract_warehouses(self, lower_question: str) -> List[str]:
        # ================================
        # Function: _extract_warehouses
        # Purpose: Extracts warehouse IDs such as WH-01 from free-text questions.
        # Inputs:
        #   - lower_question (str): lowercase user question
        # Output:
        #   - List[str] of valid warehouse IDs present in the data
        # ================================
        matches = [match.upper() for match in re.findall(r"wh[- ]?\d{2}", lower_question)]
        normalized = []
        for match in matches:
            match = match.replace(" ", "-")
            if not match.startswith("WH-"):
                match = match.replace("WH", "WH-")
            normalized.append(match)
        return [warehouse for warehouse in normalized if warehouse in self.available_warehouses]

    def _detect_drilldown(self, lower_question: str) -> Optional[str]:
        for alias, drilldown_id in sorted(DRILLDOWN_ALIASES.items(), key=lambda item: len(item[0]), reverse=True):
            if alias in lower_question:
                return drilldown_id
        return None

    def _detect_time_grain(self, lower_question: str) -> Optional[str]:
        for alias, grain in TIME_GRAIN_ALIASES.items():
            if alias in lower_question:
                return grain
        return None

    def _drilldown_response(
        self,
        *,
        kpi: Optional[str],
        domain: Optional[str],
        drilldown_id: str,
        warehouses: Sequence[str],
        time_grain: Optional[str],
        lower_question: str,
    ) -> str:
        target_domain = self.kpi_to_section.get(kpi) if kpi else domain
        if not target_domain:
            target_domain = domain
        if not target_domain or target_domain not in self.sections:
            return "Specify a KPI section or KPI name for the drill-down, for example 'show stockout exposure by SKU family'."

        drilldown = self.sections[target_domain].get("drilldowns", {}).get(drilldown_id)
        if not drilldown:
            return f"{target_domain} does not expose a {drilldown_id.replace('_', ' ')} drill-down in the current payload."
        if not drilldown.get("available"):
            return f"{target_domain} {drilldown.get('label', drilldown_id)} is unavailable: {drilldown.get('unavailable_reason', 'dimension not present in source data')}."

        active_table = drilldown
        grain_label = ""
        if drilldown_id == "by_date":
            selected_grain = time_grain or "day"
            grain_tables = drilldown.get("grain_tables", {})
            active_table = grain_tables.get(selected_grain, drilldown)
            grain_label = f" ({selected_grain})"

        rows = list(active_table.get("rows", []))
        if warehouses and "warehouse_id" in active_table.get("group_by", []):
            rows = [row for row in rows if row.get("warehouse_id") in warehouses]

        if kpi:
            rows = [row for row in rows if kpi in row.get("metrics", {})]
        if not rows:
            scope_text = f" for {', '.join(warehouses)}" if warehouses else ""
            kpi_text = f" for {kpi}" if kpi else ""
            return f"No {target_domain} {drilldown.get('label', drilldown_id)} rows are available{scope_text}{kpi_text} in the current reporting scope."

        if kpi:
            if any(term in lower_question for term in ["causing", "driver", "drivers", "cause"]):
                ranked = sorted(rows, key=lambda item: item["metrics"][kpi]["value"], reverse=True)
                top_row = ranked[0]
                dimension_text = ", ".join(f"{column}={top_row.get(column)}" for column in active_table.get("group_by", []))
                metric = top_row["metrics"][kpi]
                return (
                    f"Top driver for {kpi} in {target_domain}{grain_label} is {dimension_text}: "
                    f"{metric['display_value']} ({metric['status']})."
                )

            formatted_rows = []
            for row in rows[:10]:
                dimension_text = ", ".join(f"{column}={row.get(column)}" for column in active_table.get("group_by", []))
                metric = row["metrics"][kpi]
                formatted_rows.append(f"{dimension_text}: {metric['display_value']} ({metric['status']})")
            return f"{target_domain} {kpi} by {drilldown.get('label', drilldown_id)}{grain_label}: " + "; ".join(formatted_rows)

        preview_rows = []
        for row in rows[:5]:
            dimension_text = ", ".join(f"{column}={row.get(column)}" for column in active_table.get("group_by", []))
            preview_rows.append(dimension_text)
        return f"{target_domain} {drilldown.get('label', drilldown_id)}{grain_label} is available for {self.payload['reporting_period']['label']}. Example rows: " + "; ".join(preview_rows)

    def _detect_domain(self, lower_question: str) -> Optional[str]:
        # ================================
        # Function: _detect_domain
        # Purpose: Maps user wording to a canonical KPI section name.
        # Inputs:
        #   - lower_question (str): lowercase user question
        # Output:
        #   - Optional[str] canonical domain name
        # ================================
        for alias, canonical in sorted(DOMAIN_ALIASES.items(), key=lambda item: len(item[0]), reverse=True):
            if alias in lower_question:
                return canonical
        return None

    def _detect_kpi(self, lower_question: str) -> Optional[str]:
        # ================================
        # Function: _detect_kpi
        # Purpose: Maps user wording to a canonical KPI name.
        # Inputs:
        #   - lower_question (str): lowercase user question
        # Output:
        #   - Optional[str] canonical KPI name
        # ================================
        for alias, canonical in sorted(KPI_ALIASES.items(), key=lambda item: len(item[0]), reverse=True):
            if alias in lower_question:
                return canonical
        for canonical in self.kpi_lookup:
            if canonical.lower() in lower_question:
                return canonical
        return None
