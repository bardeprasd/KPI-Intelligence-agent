
# AI Agent for Aftersales Operations — Candidate Assignment

**Objective:** Design a Python-based AI Agent that consolidates KPIs across Inbound, Outbound, Inventory, Warehouse Productivity, and Employee Productivity into a **single-page executive summary** and provides an implementation plan.

**Timebox:** 4–6 hours (aim for MVP quality with clear assumptions).

## Deliverables
1. **Data Model:** Conceptual description of entities and relationships; table definitions with columns/types/grain and sample data (use the CSVs in `data/`).
2. **KPI Dictionary:** Name, definition, formula (pseudo-SQL), source tables, frequency, KPI owner.
3. **One-Page Layout:** Exact layout spec + sample values for latest month.
4. **Python Agent Plan:** Architecture, step-by-step plan, and code skeleton to ingest → compute KPIs → generate summary (text + data structure) → export to Excel/HTML.
5. **Requirements Doc:** Functional & non-functional requirements.
6. **Output Contract:** JSON schema for the one-pager output.

## What We Provide
- **Sample CSVs** in `data/` for prototyping (5–10 rows each).
- A **templates** folder with a sample Excel structure for the one-pager (KPI & Insights sheets).
- This assignment brief and support docs in `docs/`.

## What You Submit
- A zipped folder with your **Markdown/Doc** outputs and **Python code skeleton** (you may extend `agent/`).
- A short `NOTES.md` describing key assumptions, trade-offs, and next steps.

## Evaluation Focus
- Clarity and practicality of data model & KPIs.
- Correctness and transparency of KPI formulas.
- Sound Python plan and modular code structure.
- Ability to produce an executive one-pager (spec + sample values).

