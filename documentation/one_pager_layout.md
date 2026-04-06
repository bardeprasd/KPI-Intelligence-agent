# One-Pager Layout

## Design Goal
Provide a single-page executive summary that can be scanned in under two minutes while still allowing operations leaders to identify the most urgent issues and next actions.

## Page Structure

### 1. Header
**Placement:** top of page, full width  
**Fields:**
- title: `Aftersales Operations KPI Intelligence Summary`
- subtitle: `Deterministic monthly leadership view across inbound, outbound, inventory, warehouse, and workforce performance`
- reporting period
- warehouse scope
- generated timestamp
- overall status badge

### 2. Executive Summary Cards
**Placement:** below header, single row of 5 cards  
**Recommended cards:**
1. Fill Rate %
2. OTIF %
3. Days of Supply
4. Lines Picked per Labor-Hour
5. Error Rate %

**Card fields:**
- KPI label
- current value
- target
- status color
- one short note

## 3. Functional Sections
Two-column grid, compact card layout.

### Inbound Section
Display:
- Average Inbound Lead Time
- Receipts On-Time %
- Quantity Discrepancy %
- Inbound Volume
- section insight
- section risk level

### Outbound Section
Display:
- Fill Rate %
- OTIF %
- Backorder Rate %
- Outbound Volume
- section insight
- section risk level

### Inventory Section
Display:
- Days of Supply
- Stockout Exposure %
- Safety Stock Coverage %
- Aged Inventory % (>180d)
- section insight
- section risk level

### Warehouse Productivity Section
Display:
- Lines Picked per Labor-Hour
- Orders Processed per Labor-Hour
- SLA Adherence %
- Equipment Utilization %
- section insight
- section risk level

### Employee Productivity Section
Display:
- Picks per Person per Hour
- Error Rate %
- Rework Rate %
- Overtime %
- section insight
- section risk level

## 4. KPI Summary Table
**Placement:** bottom half of page, full width  
**Columns:**
- KPI
- Domain
- Value
- Unit
- Target
- Status
- Trend / note

The table should be sortable in future digital implementations, but static and compact in the assignment output.

## 5. Insights, Risks, and Recommendations
**Placement:** bottom of page, 3-column block or stacked sections  
**Content:**
- Top 3 insights
- Top 3 operational risks
- Top 3 recommended actions

Each bullet should be short and action-oriented.

## 6. Visual Guidelines
- Keep the page limited to one screen / one printable page where possible
- Use status colors consistently: green, amber, red
- Use concise metric formatting
- Avoid dense narrative paragraphs
- Favor operational language over technical jargon

## 7. Example Narrative Placement
- `Insight:` Service performance is under target, driven by low OTIF and elevated backorders.
- `Risk:` Inventory stockout exposure is contributing to customer service risk.
- `Recommendation:` Rebalance safety stock for high-backorder SKUs and review late-shipment causes by warehouse.

## 8. Mapping to Output Contract
| Layout Region | JSON Section |
|---|---|
| Header | `header`, `reporting_period` |
| Summary cards | `summary_cards` |
| Functional cards | `sections` |
| KPI table | `kpi_table` |
| Insights / risks / recommendations | `insights`, `recommendations` |
| Footer / audit | `metadata`, `audit` |
