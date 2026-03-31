# KPI Dictionary

## KPI Design Rules
- All KPIs are computed deterministically from source data.
- Division by zero returns `0.0` and is flagged through validation warnings when material.
- Percentages are stored as ratios in code and rendered as `%` in outputs.
- Weighted KPIs prefer numerator/denominator recomputation over averaging precomputed rates.
- Status is assigned using target thresholds from `agent/config.py`.

## Inbound KPIs

| KPI Name | Business Definition | Formula | Source Table | Grain | Reporting Frequency | KPI Owner | Target / Threshold | Notes / Assumptions |
|---|---|---|---|---|---|---|---|---|
| Average Inbound Lead Time | Average days between expected and actual receipt for receipts completed in the reporting period | `AVG(inbound_lead_time_days)` | inbound_parts | Reporting period | Weekly / Monthly | Inbound Operations | <= 5 days good, <= 8 amber, else red | Uses provided operational lead-time field |
| Receipts On-Time % | Share of receipts received on or before expected date | `COUNT(received_date <= expected_date) / COUNT(*)` | inbound_parts | Reporting period | Weekly / Monthly | Inbound Operations | >= 95% good, >= 90% amber | Uses receipt-line count |
| Quantity Discrepancy % | Share of ordered inbound quantity not received as expected | `SUM(discrepancy_qty) / SUM(qty_ordered)` | inbound_parts | Reporting period | Weekly / Monthly | Quality | <= 2% good, <= 5% amber | Higher is worse |
| Inbound Volume | Total quantity received in period | `SUM(qty_received)` | inbound_parts | Reporting period | Daily / Weekly / Monthly | Inbound Operations | Informational | Volume only, no target |
| Late Receipt Count | Number of receipt lines arriving after expected date | `COUNT(received_date > expected_date)` | inbound_parts | Reporting period | Weekly / Monthly | Purchasing | Informational | Supports supplier risk insight |
| Top 5 Delaying Suppliers | Top suppliers ranked by late ordered quantity in the reporting period | `TOP 5 supplier_name BY SUM(qty_ordered) WHERE received_date > expected_date` | inbound_parts | Reporting period | Monthly | Purchasing | Informational | Rendered as ranked text in outputs |

## Outbound KPIs

| KPI Name | Business Definition | Formula | Source Table | Grain | Reporting Frequency | KPI Owner | Target / Threshold | Notes / Assumptions |
|---|---|---|---|---|---|---|---|---|
| Fill Rate % | Share of ordered outbound quantity successfully shipped | `SUM(qty_shipped) / SUM(qty_ordered)` | outbound_parts | Reporting period | Daily / Weekly / Monthly | Customer Service | >= 95% good, >= 90% amber | Recomputed rather than trusting provided fill_rate |
| OTIF % | Share of shipped order lines marked on-time and in-full | `SUM(otif_flag) / COUNT(*)` | outbound_parts | Reporting period | Weekly / Monthly | Logistics | >= 92% good, >= 85% amber | Uses supplied OTIF flag |
| Backorder Rate % | Share of ordered outbound quantity remaining on backorder | `SUM(backorder_qty) / SUM(qty_ordered)` | outbound_parts | Reporting period | Weekly / Monthly | Supply Planning | <= 3% good, <= 7% amber | Higher is worse |
| Outbound Volume | Total quantity shipped in period | `SUM(qty_shipped)` | outbound_parts | Reporting period | Daily / Weekly / Monthly | Logistics | Informational | Supports service context |
| Late Shipment Count | Number of shipments after promise date | `COUNT(shipped_date > promise_date)` | outbound_parts | Reporting period | Weekly / Monthly | Logistics | Informational | Supports OTIF diagnosis |
| Top 10 SKUs by Backorder | Top SKUs ranked by backordered quantity in the reporting period | `TOP 10 part_number BY SUM(backorder_qty)` | outbound_parts | Reporting period | Weekly / Monthly | Supply Planning | Informational | Rendered as ranked text in outputs |

## Inventory KPIs

| KPI Name | Business Definition | Formula | Source Table | Grain | Reporting Frequency | KPI Owner | Target / Threshold | Notes / Assumptions |
|---|---|---|---|---|---|---|---|---|
| Days of Supply | Average available days of supply across inventory records in period | `AVG(days_of_supply)` | inventory_snapshot | Reporting period | Daily / Weekly / Monthly | Supply Planning | >= 20 and <= 45 good; 10-20 or 45-60 amber; else red | Using provided operational metric |
| Stockout Exposure % | Share of inventory snapshot rows flagged as stockout | `SUM(stockout_flag) / COUNT(*)` | inventory_snapshot | Reporting period | Daily / Weekly / Monthly | Inventory Control | <= 2% good, <= 5% amber | Higher is worse |
| Safety Stock Coverage % | Share of snapshot rows with available quantity at or above safety stock | `COUNT(available_qty >= safety_stock) / COUNT(*)` | inventory_snapshot | Reporting period | Weekly / Monthly | Supply Planning | >= 95% good, >= 90% amber | Measures buffer sufficiency |
| Aged Inventory % (>180 days) | Share of inventory records older than 180 days | `COUNT(age_days > 180) / COUNT(*)` | inventory_snapshot | Reporting period | Monthly | Inventory Control | <= 15% good, <= 25% amber | Higher is worse |
| Average Inventory Age | Mean inventory age across snapshot rows | `AVG(age_days)` | inventory_snapshot | Reporting period | Monthly | Inventory Control | Informational | Useful for obsolescence context |
| Inventory Turns | Annualized outbound flow relative to average on-hand inventory | `ANNUALIZED SUM(qty_shipped) / AVG(on_hand_qty)` | inventory_snapshot, outbound_parts | Reporting period | Monthly | Supply Planning | Informational | Proxy because finance valuation fields are not present |
| Aged Inventory Value >180d | Value proxy for inventory older than 180 days | `SUM(on_hand_qty * unit_cost) WHERE age_days > 180; proxy to SUM(on_hand_qty) when unit_cost is unavailable` | inventory_snapshot | Reporting period | Monthly | Inventory Control | Informational | Current data model falls back to aged quantity when `unit_cost` is unavailable |

## Warehouse Productivity KPIs

| KPI Name | Business Definition | Formula | Source Table | Grain | Reporting Frequency | KPI Owner | Target / Threshold | Notes / Assumptions |
|---|---|---|---|---|---|---|---|---|
| Lines Picked per Labor-Hour | Warehouse throughput normalized by labor consumption | `SUM(lines_picked) / SUM(labor_hours)` | warehouse_productivity | Reporting period | Daily / Weekly / Monthly | Warehouse Operations | >= 14 good, >= 12 amber | Weighted productivity rate |
| Orders Processed per Labor-Hour | Order throughput normalized by labor consumption | `SUM(orders_processed) / SUM(labor_hours)` | warehouse_productivity | Reporting period | Daily / Weekly / Monthly | Warehouse Operations | >= 1.2 good, >= 1.0 amber | Weighted throughput rate |
| Orders per Day | Average order volume processed per operating day in period | `SUM(orders_processed) / COUNT(DISTINCT date)` | warehouse_productivity | Reporting period | Daily / Weekly / Monthly | Warehouse Operations | Informational | Useful for volume context alongside productivity rates |
| SLA Adherence % | Average service-level adherence achieved during the period | `AVG(sla_adherence_pct)` | warehouse_productivity | Reporting period | Daily / Weekly / Monthly | Warehouse Operations | >= 95% good, >= 90% amber | Uses upstream service metric |
| Equipment Utilization % | Average equipment utilization during the period | `AVG(equipment_utilization_pct)` | warehouse_productivity | Reporting period | Daily / Weekly / Monthly | Warehouse Operations | >= 75% and <= 90% good; 65-75 amber; else red | Very low or very high can both be concerning |
| Touches per Order | Average operational touches required to process an order | `AVG(touches_per_order)` | warehouse_productivity | Reporting period | Weekly / Monthly | Warehouse Operations | <= 3.5 good, <= 4.0 amber | Lower is better |

## Employee Productivity KPIs

| KPI Name | Business Definition | Formula | Source Table | Grain | Reporting Frequency | KPI Owner | Target / Threshold | Notes / Assumptions |
|---|---|---|---|---|---|---|---|---|
| Picks per Person per Hour | Employee pick productivity normalized by hours worked | `SUM(picks) / SUM(hours_worked)` | employee_productivity | Reporting period | Daily / Weekly / Monthly | Workforce Management | >= 14 good, >= 12 amber | Weighted rate across employees |
| Error Rate % | Share of completed tasks resulting in errors | `SUM(errors) / SUM(tasks_completed)` | employee_productivity | Reporting period | Weekly / Monthly | Quality | <= 1.0% good, <= 2.0% amber | Lower is better |
| Rework Rate % | Share of completed tasks requiring rework | `SUM(rework) / SUM(tasks_completed)` | employee_productivity | Reporting period | Weekly / Monthly | Quality | <= 1.5% good, <= 3.0% amber | Lower is better |
| Overtime % | Share of total worked time that is overtime | `SUM(overtime_hours) / SUM(hours_worked + overtime_hours)` | employee_productivity | Reporting period | Weekly / Monthly | Workforce Management | <= 8% good, <= 12% amber | Lower is generally healthier |
| Average Tasks per Employee | Average tasks completed per employee record | `AVG(tasks_completed)` | employee_productivity | Reporting period | Weekly / Monthly | Workforce Management | Informational | Helps contextualize quality and overtime |

## Ownership Guidance
| Domain | Typical KPI Owner |
|---|---|
| Inbound | Purchasing / Inbound Operations |
| Outbound | Customer Service / Logistics |
| Inventory | Supply Planning / Inventory Control |
| Warehouse Productivity | Warehouse Operations |
| Employee Productivity | Operations / Workforce Management |

## Traceability Guidance
Each KPI in the output contract should include:
- source dataset
- formula description
- target used
- status logic used
- reporting period applied
- warehouse filter applied (if any)

That traceability is included in the Python output payload.
