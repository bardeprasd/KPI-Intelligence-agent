
# Python AI Agent — Architecture & Implementation Plan

## High-Level Architecture (text diagram)

[User Input]
  → Date range, Warehouses, Filters (SKU families)

[Ingestion Layer]
  - Sources: CSV/DB/API/BI extracts
  - Modules: `agent.ingest`

[Transform & Aggregate]
  - Clean, join, derive: `agent.kpi`
  - Persist intermediate tables (parquet/csv)

[KPI Computation]
  - Functions per domain computing metrics with transparent formulas

[Summarization Layer]
  - LLM-assisted narrative generation using KPI dictionary and trends: `agent.summarize`

[Output Layer]
  - Write: Excel (openpyxl), HTML/PDF (optional), JSON contract mapping to one-page layout: `agent.output`

[Agent Orchestration]
  - `run_agent.py` orchestrates steps; designed to plug into Copilot Studio/Agentforce later.

## Implementation Roadmap
1. **Environment Setup:** Create virtual env; install pandas, numpy, openpyxl, scikit-learn (optional), jinja2 (optional), matplotlib/plotly.
2. **Data Contracts:** Freeze table schemas (CSV headers) and add validation checks in `ingest.validate()`.
3. **KPI Functions:** Implement functions for each KPI; add unit tests with the sample data.
4. **One-Pager Layout:** Implement `output.build_layout_json()` to map KPI results to cards/sections.
5. **LLM Summary (Optional):** Create `summarize.summarize_kpis(kpi_dict)`; keep a deterministic fallback summary.
6. **Exporters:** `output.to_excel(layout_json, path)` and `output.to_html(...)`.
7. **CLI Entry:** `python run_agent.py --start 2026-02-01 --end 2026-02-29 --warehouses WH-01,WH-02`.

