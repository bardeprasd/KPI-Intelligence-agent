"""
Optional OpenAI-based narrative refinement for KPI outputs.

This file sits in the LLM enrichment layer of the project. It sends structured,
already-computed KPI facts to OpenAI and asks for concise leadership wording
while preserving deterministic KPI values, statuses, and formulas.
"""

from __future__ import annotations

import json
import os
from typing import Dict, List, Tuple


def openai_available() -> bool:
    # ================================
    # Function: openai_available
    # Purpose: Checks whether the OpenAI API key is available in the environment.
    # Inputs:
    #   - None
    # Output:
    #   - bool indicating whether OpenAI calls can be attempted
    # ================================
    return bool(os.getenv("OPENAI_API_KEY"))


def _section_risk_level(section: Dict) -> str:
    # ================================
    # Function: _section_risk_level
    # Purpose: Recomputes section risk after LLM wording is applied.
    # Inputs:
    #   - section (Dict): section containing KPI statuses
    # Output:
    #   - str risk level: red, amber, or green
    # ================================
    statuses = [k["status"] for k in section["kpis"] if k["status"] != "info"]
    if "red" in statuses:
        return "red"
    if "amber" in statuses:
        return "amber"
    return "green"


SYSTEM_PROMPT = """You are an executive operations analyst.
Generate concise leadership-ready narrative from the structured KPI facts provided.
Do not invent facts, metrics, warehouse names, causes, trends, or recommendations not supported by the data.
Use only the structured KPI facts provided.
Return strict JSON with keys: section_insights, insights, recommendations.
- section_insights must be a list of objects with keys: section, insight
- insights must be a list of short strings
- recommendations must be a list of short strings
"""


class LLMPolishError(RuntimeError):
    # Raised when the optional LLM narrative step cannot complete safely.
    pass



def generate_narrative_with_openai(
    *,
    sections: List[Dict],
    model: str = "gpt-4.1-mini",
    max_items: int = 5,
) -> Tuple[List[Dict], List[str], List[str]]:
    # ================================
    # Function: generate_narrative_with_openai
    # Purpose: Uses OpenAI to rewrite section insights and executive narrative.
    # Inputs:
    #   - sections (List[Dict]): deterministic KPI sections
    #   - model (str): OpenAI model name
    #   - max_items (int): maximum number of returned insights/actions
    # Output:
    #   - Tuple[List[Dict], List[str], List[str]] of updated sections,
    #     insights, and recommendations
    # Important Logic:
    #   - Sends only structured KPI facts to the model
    #   - Rejects empty or invalid JSON responses
    #   - Preserves deterministic KPI values while updating narrative text only
    # ================================
    try:
        from openai import OpenAI
    except Exception as exc:  # pragma: no cover
        raise LLMPolishError(
            "OpenAI package is not installed. Install with: pip install openai"
        ) from exc

    if not openai_available():
        raise LLMPolishError("OPENAI_API_KEY is not set.")

    client = OpenAI()

    section_snapshot = []
    for section in sections:
        # Provide the model with a compact, factual snapshot instead of raw
        # source data so the response stays grounded and easy to validate.
        section_snapshot.append(
            {
                "section": section["name"],
                "risk_level": section.get("risk_level"),
                "insight": section.get("insight"),
                "kpis": [
                    {
                        "name": kpi["name"],
                        "display_value": kpi["display_value"],
                        "status": kpi["status"],
                        "target": kpi.get("target_display"),
                    }
                    for kpi in section["kpis"]
                ],
            }
        )

    user_payload = {
        "task": "Generate one-pager narrative for leadership from KPI sections.",
        "constraints": [
            "Do not add new facts or numbers.",
            "Base every statement only on the supplied KPI values, statuses, and targets.",
            "Keep each section insight to one short sentence.",
            "Keep each bullet short and executive-friendly.",
            f"Return exactly one section insight per section and at most {max_items} top insights and {max_items} recommendations.",
        ],
        "sections": section_snapshot,
    }

    # The model is instructed to return strict JSON so the narrative can be
    # merged back into the pipeline deterministically.
    try:
        response = client.responses.create(
            model=model,
            input=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
            ],
            temperature=0.2,
        )
    except Exception as exc:
        raise LLMPolishError(f"OpenAI request failed: {exc}") from exc

    text = getattr(response, "output_text", "") or ""
    if not text:
        raise LLMPolishError("OpenAI response did not contain output_text.")

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as exc:
        raise LLMPolishError(f"OpenAI response was not valid JSON: {text[:300]}") from exc

    section_insights = parsed.get("section_insights", [])
    generated_insights = parsed.get("insights", [])
    generated_recommendations = parsed.get("recommendations", [])

    if not isinstance(section_insights, list) or not isinstance(generated_insights, list) or not isinstance(generated_recommendations, list):
        raise LLMPolishError("OpenAI response JSON must contain list values for section_insights, insights, and recommendations.")

    insight_lookup: Dict[str, str] = {}
    for item in section_insights:
        if not isinstance(item, dict):
            continue
        section_name = str(item.get("section", "")).strip()
        insight_text = str(item.get("insight", "")).strip()
        if section_name and insight_text:
            insight_lookup[section_name] = insight_text

    updated_sections: List[Dict] = []
    for section in sections:
        updated = dict(section)
        updated["risk_level"] = _section_risk_level(section)
        updated["insight"] = insight_lookup.get(section["name"], section.get("insight", "No additional insight available."))
        updated_sections.append(updated)

    generated_insights = [str(item).strip() for item in generated_insights if str(item).strip()][:max_items]
    generated_recommendations = [str(item).strip() for item in generated_recommendations if str(item).strip()][:max_items]

    return updated_sections, generated_insights, generated_recommendations
