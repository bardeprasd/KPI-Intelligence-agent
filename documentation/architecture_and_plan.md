# Architecture and Plan

## 1. Objective
Build a modular KPI intelligence agent that converts warehouse and aftersales operational data into a leadership-ready one-page summary.

## 2. Why This Is an AI Agent
This solution is not a chatbot. It is an agentic workflow because it:
1. accepts an operational objective and reporting scope
2. gathers data from multiple sources
3. validates and standardizes inputs
4. computes domain-specific KPIs
5. reasons over cross-domain conditions using explicit rules
6. generates action-oriented output artifacts for human decision-makers

The system remains auditable because all critical calculations are deterministic and traceable.

## 3. Modular Architecture

```text
User / Scheduler / CLI
        |
        v
run_agent.py  -----------------------------
        |                                  |
        v                                  |
agent.ingest -> cleaned datasets           |
        |                                  |
        v                                  |
agent.kpi -> KPI sections + KPI table      |
        |                                  |
        v                                  |
agent.summarize -> insights / risks / recs  |
        |                                  |
        v                                  |
agent.llm_summary -> OpenAI-first refinement |
        |                                  |
        v                                  |
agent.output -> JSON / Excel / HTML -------
        |
        v
optional chatbot session from run_agent.py
```

## 4. Ingestion Flow
1. Discover configured source files
2. Load CSVs into pandas DataFrames
3. Normalize column names to lowercase snake_case
4. Parse date columns
5. Coerce numeric fields
6. Validate required columns and basic sanity checks
7. Return cleaned datasets, row counts, and validation warnings

## 5. Validation Flow
Validation is intentionally lightweight but practical.

### Blocking checks
- file exists
- required columns present
- critical date columns parseable

### Non-blocking warnings
- null keys
- negative quantities where unexpected
- percentage-like fields outside logical range
- shipped/received dates before order/expected dates in suspicious patterns

## 6. KPI Computation Layer
The KPI layer contains one function per domain and a combined orchestration function.

### Domain modules
- `compute_inbound_kpis`
- `compute_outbound_kpis`
- `compute_inventory_kpis`
- `compute_warehouse_productivity_kpis`
- `compute_employee_productivity_kpis`

### Shared responsibilities
- apply date filter
- apply optional warehouse filter where relevant
- compute numerators / denominators safely
- assign targets and status
- package KPI metadata consistently

## 7. Summarization Layer
The summarization layer is deterministic by default.

### Deterministic logic examples
- If inbound on-time is red and outbound OTIF is red, note likely inbound service drag.
- If stockout exposure or safety stock coverage is red, generate inventory-risk commentary.
- If overtime is elevated together with error rate, recommend workload balancing and quality review.

### Why deterministic first
- repeatable
- easy to explain in interviews
- safer for executive reporting
- aligned with KPI auditability requirements

### OpenAI-first LLM hook
The `llm_summary` step is attempted by default and can refine already-computed section insights and recommendations. It never computes KPI values directly. If OpenAI is unavailable, the pipeline falls back to deterministic narrative so report generation still completes.

## 8. Output Layer
The output layer transforms structured KPI results into three channels.

### JSON
Two JSON views are produced:
- a strict assignment-contract export for reviewer compliance
- a richer detailed payload for HTML rendering, audit context, and chatbot grounding

### Excel
Reviewer-friendly workbook with summary, detail, insights, and metadata tabs.

### HTML
Portable one-page artifact for demo or sharing.

## 9. Orchestration Design
`run_agent.py` is responsible for:
1. parsing CLI arguments
2. loading configuration
3. running ingestion
4. deriving default reporting period if not provided
5. computing KPIs
6. generating insights and recommendations
7. building the final output payload
8. writing artifacts
9. printing and saving run summary
10. optionally launching an interactive chatbot session from the same run context with `--start-chatbot`

## 10. Deterministic vs LLM-Based Logic

| Layer | Deterministic | Optional LLM |
|---|---|---|
| Ingestion | Yes | No |
| Validation | Yes | No |
| KPI computation | Yes | No |
| Status assignment | Yes | No |
| Executive narrative | Yes | Optional |
| Recommendation wording | Yes | Optional refinement only |

## 11. Why It Is Auditable
1. KPI formulas are explicit in code and documentation.
2. Targets are centralized in config.
3. Output contains metadata and audit fields.
4. Validation warnings are retained.
5. Narrative statements are grounded in observed KPI conditions.
6. No opaque model is used for core business math.

## 12. Implementation Plan
### Phase 1: MVP
- load data
- validate schemas
- compute monthly KPIs
- export JSON and Excel
- create one-page HTML

### Phase 2: Strengthen
- trend comparisons versus prior period
- richer warehouse and SKU slices
- automated tests
- packaging / CI

### Phase 3: Productionize
- DB/API connectors
- orchestration scheduling
- historical metric store
- alerting and access control
- optional governed LLM narrative layer

## 13. Leadership Discussion Positioning
This architecture balances **agentic behavior** with **enterprise trust**. It shows that the candidate understands when to use AI-style orchestration and when not to overuse LLMs for structured analytics.


## Optional OpenAI narrative layer

The architecture supports an OpenAI-first narrative refinement step after deterministic KPI computation and summarization.
This layer is intentionally isolated from the calculation engine.

Flow:
1. ingest and validate source data
2. compute KPI values deterministically
3. assign targets and statuses deterministically
4. generate base section insights and recommendations with deterministic rules
5. attempt to send those finished bullets to OpenAI for refinement
6. write JSON, Excel, HTML outputs
7. optionally launch the chatbot from `run_agent.py --start-chatbot`

Why this is safe and auditable:
- KPI math remains deterministic
- thresholds remain config-driven
- section risk levels remain deterministic
- LLM output is grounded on computed facts, not free-form business math
