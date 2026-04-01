
# Requirements — AI Agent & Consolidated Leadership View

## 1. Introduction & Objectives
Provide a single consolidated leadership summary across inbound, outbound, inventory, warehouse and employee productivity, with drill-down potential and narrative insights.

## 2. Scope
- **Datasets:** inbound_parts, outbound_parts, inventory_snapshot, warehouse_productivity, employee_productivity.
- **Warehouses:** Multi-warehouse, global-ready (IDs and names as attributes).

## 3. Users & Personas
- **Leadership:** VP/Director Aftersales, Operations Head.
- **Analytics/Ops:** Build, validate, and interpret KPIs.

## 4. Functional Requirements
- Ingest CSV/DB/API sources on a scheduled cadence.
- Compute KPI dictionary deterministically with traceable formulas.
- Generate a one-page summary (Excel/HTML/JSON) per period and warehouse set.
- Optional NL Q&A over computed data (post-MVP).

## 5. Non-Functional Requirements
- **Performance:** Monthly refresh < 10 minutes for 2 years data, 10 warehouses, 50k SKUs.
- **Security:** No PII; role-based access to outputs; file-system and DB credentials via secrets.
- **Auditability:** Every KPI shows source table, period filter, and calculation notes.

## 6. Data Governance & Quality
- Schema validation; type checks; non-null checks for keys and dates.
- Outlier detection on lead time, fill rate; missing values flagged.

## 7. Assumptions & Constraints
- Initial sources provided as CSV extracts; later DB/API connectors.
- Costs/COGS not included in sample; may be joined from Finance later.

## 8. Risks & Mitigations
- **Inconsistent definitions** → Maintain KPI dictionary as source of truth.
- **Data latency** → Timestamp all extracts; alert on staleness.
- **Model drift (LLM)** → Keep deterministic fallback summaries.

## 9. Create a single page visual using python
- Using the output result, create a report to showcase the result visually usin one_pager_layout.md file