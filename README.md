# AI Agent Leadership Assignment Submission

## Overview
This submission delivers a deterministic, auditable KPI intelligence agent for warehouse and aftersales operations. The agent ingests synthetic operational datasets, validates and standardizes them, computes leadership-ready KPIs across five business domains, generates rule-based insights and recommendations, and exports the results as JSON, Excel, and HTML.

The design keeps the KPI engine deterministic and reproducible, while supporting an OpenAI-first layer for narrative refinement and grounded chatbot responses. This preserves auditability while still demonstrating how LLM-assisted interaction can sit on top of structured KPI computation.

## Business Objective
Create a one-page executive summary that consolidates:
- Inbound operations
- Outbound operations
- Inventory health
- Warehouse productivity
- Employee productivity

The output is suitable for leadership review, operational triage, and audit-friendly handoff to analytics or BI teams.

## Key Design Principles
- Deterministic first: no KPI is computed by an LLM
- Auditable: every KPI includes formula, target, and traceability metadata
- Modular: ingestion, KPI logic, summarization, and output are separate modules
- Extensible: new sources, KPIs, dimensions, and exporters can be added with minimal impact
- Business-friendly: output is formatted for leadership consumption, not only developer inspection

## Project Structure
```text
AI_Agent_Leadership_Assignment_Submission/
├── README.md
├── NOTES.md
├── requirements.txt
├── run_agent.py
├── chatbot_demo.py
├── agent/
│   ├── __init__.py
│   ├── config.py
│   ├── ingest.py
│   ├── kpi.py
│   ├── output.py
│   ├── chatbot.py
│   └── llm_summary.py
├── docs/
│   ├── architecture_and_plan.md
│   ├── data_model.md
│   ├── kpi_dictionary.md
│   ├── one_pager_layout.md
│   ├── output_schema.json
│   ├── chatbot_feasibility.md
│   └── requirements.md
├── data/
│   ├── employee_productivity.csv
│   ├── inbound_parts.csv
│   ├── inventory_snapshot.csv
│   ├── outbound_parts.csv
│   └── warehouse_productivity.csv
├── templates/
│   └── one_pager_example.xlsx
└── output/
    ├── leadership_summary.json
    ├── kpi_summary.xlsx
    ├── one_pager.html
    ├── chatbot_demo_transcript.md
    └── run_summary.txt
```

## Setup
### 1. Create a virtual environment
```bash
python -m venv .venv
```

### 2. Activate it
Windows:
```bash
.venv\Scripts\activate
```

macOS/Linux:
```bash
source .venv/bin/activate
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

## How to Run
### Default run
Runs the latest fully available common reporting month across all domains.
OpenAI narrative refinement is attempted by default and falls back cleanly if OpenAI is unavailable.
```bash
python run_agent.py
```

### Run report and immediately open the chatbot
This is the best demo flow because it generates the artifacts first and then launches a grounded chat session on the same payload.
```bash
python run_agent.py --start-chatbot
```

### Explicit reporting window
```bash
python run_agent.py --start 2026-02-01 --end 2026-02-28
```

### Warehouse filter
```bash
python run_agent.py --warehouses WH-01,WH-02
```

### SKU family filter
```bash
python run_agent.py --sku-families Engine,Electrical
```

### Disable HTML export
```bash
python run_agent.py --no-html
```

### Force deterministic summary wording
```bash
python run_agent.py --deterministic-summary
```

### Run report and chatbot with deterministic chat responses
```bash
python run_agent.py --start-chatbot --deterministic-chat
```

## What the Agent Does
1. Loads all CSV sources from `data/`
2. Standardizes column names and parses dates
3. Validates required columns and key data-quality conditions
4. Derives a default reporting period
5. Computes KPI sections for inbound, outbound, inventory, warehouse productivity, and employee productivity
6. Assigns status against configured targets
7. Generates deterministic insights, risks, and recommendations
8. Builds a unified JSON contract
9. Exports JSON, Excel, HTML, and a console/text run summary


## Chatbot Demo
In addition to the KPI one-pager, this submission includes a lightweight chatbot layer that can:
- answer natural-language KPI questions
- summarize sections such as Inventory or Outbound
- compare warehouse performance for warehouse-grained KPIs
- preserve conversational context across follow-up turns

### Run the chatbot demo
OpenAI-backed chat is attempted by default and falls back cleanly to deterministic answers if OpenAI is unavailable.

Interactive mode:
```bash
python chatbot_demo.py
```

Single question:
```bash
python chatbot_demo.py --query "What is the fill rate?"
```

Scripted demo transcript:
```bash
python chatbot_demo.py --demo-script --save-transcript output/chatbot_demo_transcript.md
```

Deterministic-only chat mode:
```bash
python chatbot_demo.py --deterministic-chat
```

Example contextual-memory flow:
1. `Compare WH-01 and WH-02 on SLA.`
2. `How about WH-03?`

The second question inherits the KPI context from the first turn and answers for the new warehouse.

## Generated Outputs
### `output/leadership_summary.json`
Strict assignment-contract JSON aligned to the provided `docs/output_schema.json`.

### `output/leadership_summary_detailed.json`
Richer machine-readable payload used by the HTML report and chatbot, including reporting period, section KPIs, insights, risks, recommendations, metadata, and audit details.

### `output/kpi_summary.xlsx`
Formatted Excel workbook with:
- `KPI_Summary`
- `Section_Detail`
- `Insights_Actions`
- `Metadata`

### `output/one_pager.html`
Single-page leadership view for lightweight demo and review, including header, top KPI cards, five business sections, KPI table, insights, risks, and recommendations.

### `output/run_summary.txt`
Text summary of row counts, reporting period, validation warnings, and output files generated.

## Assumptions
- The sample CSVs are source-of-truth extracts for this assignment
- Reporting period defaults to the latest full month available across the operational event dates
- Targets and thresholds are configurable and illustrative for assignment purposes
- Inventory valuation metrics are intentionally excluded because unit cost / COGS are not present in the provided data
- Narrative generation is deterministic by default and can be optionally refined with OpenAI from the computed KPI payload

## Testing / Review Guidance
To review the solution quickly:
1. Run `python run_agent.py`
2. Open `output/kpi_summary.xlsx`
3. Open `output/one_pager.html`
4. Inspect `output/leadership_summary.json` for the strict assignment contract
5. Inspect `output/leadership_summary_detailed.json` for the richer audit-friendly payload
6. Read `NOTES.md` and `docs/architecture_and_plan.md` for assumptions and extension plan


## Optional OpenAI narrative layer

This submission keeps KPI calculations and base narrative deterministic and auditable.
OpenAI is attempted by default for leadership-facing narrative refinement and grounded chatbot responses, with deterministic fallback when OpenAI is unavailable.

### OpenAI-first summary behavior

Install dependencies:
```bash
pip install -r requirements.txt
```

Set your API key.

Windows:
```bash
set OPENAI_API_KEY=your_api_key_here
```

Mac/Linux:
```bash
export OPENAI_API_KEY=your_api_key_here
```

Specify a model explicitly:
```bash
python run_agent.py --llm-model gpt-4.1-mini
```

Disable OpenAI summary refinement and keep deterministic wording only:
```bash
python run_agent.py --deterministic-summary
```

### Important guardrail

The OpenAI layer never computes KPIs, thresholds, or status logic.
It only rewrites or answers from already-computed KPI payloads.
