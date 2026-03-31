# NOTES

## Assumptions
1. **Latest common reporting month** is used as the default reporting window. This avoids mixing partial data across domains.
2. **Operational event dates** drive period filtering:
   - inbound: `received_date`
   - outbound: `shipped_date`
   - inventory: `snapshot_date`
   - warehouse productivity: `date`
   - employee productivity: `date`
3. **Thresholds are configurable defaults** chosen for a leadership demo and not represented as enterprise-approved targets.
4. **Inventory days of supply** uses the provided `days_of_supply` field from the inventory dataset rather than reconstructing demand from outbound history, because the assignment data already includes the operationally maintained metric.
5. **Fill rate** is recomputed from quantities and not trusted solely from the provided `fill_rate` column, to keep formulas consistent.
6. **OTIF** uses the provided `otif_flag` because it is an explicit domain indicator likely derived upstream from both timeliness and completeness checks.
7. **Aging** is measured in days and summarized via average age and the percentage of inventory records older than 180 days.
8. **One-pager trends** are expressed as notes and status, not time-series sparklines, because the assignment emphasizes a compact leadership view.
9. **OpenAI-first execution** is now the default for narrative refinement and chatbot responses, with deterministic fallback retained so demos still run when network/API access is unavailable.

## Trade-offs
1. **Deterministic-first narrative with optional LLM refinement**
   - Chosen to maximize auditability and reduce hallucination risk
   - OpenAI can optionally improve wording, but the KPI math and base recommendations stay deterministic
2. **CSV-first ingestion**
   - Ideal for assignment portability and reproducibility
   - Not a replacement for production-grade connectors, scheduling, and observability
3. **Config-driven targets**
   - Keeps thresholds external to KPI functions
   - Requires business calibration before real-world deployment
4. **Latest-month snapshot output**
   - Good for one-page leadership reporting
   - Does not yet include rolling 3-month or year-over-year trend analysis
5. **No complex dimensional star-schema persistence**
   - The conceptual mart is documented fully
   - The implementation computes on in-memory pandas DataFrames for simplicity and speed

## Limitations
1. Unit cost, revenue, and COGS are not available, so financial inventory KPIs are excluded.
2. The data quality framework is lightweight; it validates schema, range checks, and basic outlier warnings but is not a full enterprise DQ engine.
3. Cross-domain causal statements are conservative and rules-based; they indicate likely relationships, not statistical causality.
4. Excel output is formatted for readability, not pixel-perfect BI publishing.
5. The HTML one-pager is intentionally simple and self-contained.

## Future Enhancements
1. Add historical trend comparison (prior month, rolling 3 months, YoY)
2. Persist curated fact and dimension tables to parquet or a SQL warehouse
3. Expand anomaly detection beyond basic outlier warnings into richer degradation monitoring
4. Add deeper drill-through outputs by warehouse, supplier, SKU family, and shift
5. Add configurable alert rules and notification hooks
6. Add dbt-style tests or pytest-based unit tests
7. Add a controlled LLM summarization layer that reads only computed KPI JSON and never raw source data
8. Add Power BI / Tableau export mappings or API publishing endpoints

## Demo Guidance
For User, the best end-to-end command is:
```bash
python run_agent.py --start-chatbot
```

This shows:
1. KPI generation
2. output artifact creation
3. immediate grounded chatbot interaction on the same payload

If network access is unavailable, deterministic-only demo mode is:
```bash
python run_agent.py --start-chatbot --deterministic-summary --deterministic-chat
```
