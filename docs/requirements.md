# Requirements

## 1. Functional Requirements
1. Load all provided CSV datasets from the configured data directory.
2. Validate each source for required columns, parseable dates, and basic numeric integrity.
3. Support filtering by reporting start date, end date, and optional warehouse list.
4. Compute deterministic KPIs across:
   - inbound
   - outbound
   - inventory
   - warehouse productivity
   - employee productivity
5. Assign each KPI a target and health status.
6. Generate a one-page leadership summary with summary cards and section-level detail.
7. Produce structured JSON output matching the published schema.
8. Export a readable Excel workbook suitable for review.
9. Optionally render a lightweight HTML one-pager.
10. Record metadata including source file names, row counts, validation warnings, and calculation version.

## 2. Non-Functional Requirements
### Auditability
- Every KPI must have an explicit formula and source mapping.
- Narrative insights must be deterministic and reproducible.
- Outputs must include assumptions and validation warnings.

### Maintainability
- Code must be modular and readable.
- Business thresholds must be config-driven.
- Domain logic must be isolated from exporters.

### Reproducibility
- Running the agent on the same data and parameters should reproduce the same outputs.
- Default reporting window logic must be consistent and explainable.

### Performance
- Should complete quickly on assignment-scale CSV data.
- Design should be extensible to moderate warehouse analytics volumes through chunking or database pushdown in future iterations.

### Extensibility
- New KPI domains should be addable without rewriting orchestration.
- New output channels should be addable without changing KPI logic.
- New data connectors should be swappable into the ingestion layer.

## 3. Validation Requirements
1. Required columns must exist for every dataset.
2. Date columns must parse successfully.
3. Key identifier fields must not be entirely null.
4. Numeric quantity fields should be coerced safely to numeric types.
5. Negative quantities, invalid percentages, or date anomalies should generate warnings.
6. Division-by-zero scenarios must be handled gracefully.
7. Validation failures for missing critical columns should stop the run with a clear error.

## 4. Output Requirements
### JSON
The JSON output must include:
- header
- reporting_period
- summary_cards
- sections
- kpi_table
- insights
- recommendations
- metadata
- audit

### Excel
Workbook should include:
- KPI summary view
- section detail view
- insights and actions
- metadata / audit view

### HTML
A compact executive one-pager should present:
- reporting header
- summary KPI cards
- domain sections
- insight / risk / action bullets
- KPI table

## 5. Scalability Considerations
1. Replace CSV ingestion with database or API connectors for production.
2. Push heavy aggregations into SQL or Spark when data volume grows.
3. Persist curated marts to avoid repeated cleansing on every run.
4. Add scheduled runs and artifact versioning.
5. Add partitioned processing by reporting period and warehouse.

## 6. Auditability Considerations
1. Keep KPI formulas in code and documentation aligned.
2. Include target thresholds in config rather than hardcoding throughout the codebase.
3. Keep calculation version in metadata.
4. Store validation warnings in output payload and run summary.
5. Treat narrative generation as derived commentary, never as a substitute for calculation logic.

## 7. Deterministic vs AI-Led Behavior
This solution qualifies as an AI-agent style workflow because it orchestrates ingestion, validation, reasoning over KPIs, and recommendation generation. However, it intentionally uses deterministic business logic for all measurable outputs. Any future LLM use must remain optional, bounded, and non-authoritative for KPI math.
