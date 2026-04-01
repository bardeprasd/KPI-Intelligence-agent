
# KPI Dictionary

| Area | KPI | Definition | Formula (SQL-like) | Source Tables | Frequency | Owner |
|---|---|---|---|---|---|---|
| Inbound | Avg Inbound Lead Time (days) | Avg days between expected_date and received_date for receipts in period | AVG(DATEDIFF(day, expected_date, received_date)) | inbound_parts | Weekly/Monthly | Ops Analytics |
| Inbound | % Receipts On-Time | On-time receipts / total receipts | SUM(CASE WHEN received_date <= expected_date THEN 1 ELSE 0 END) / COUNT(*) | inbound_parts | Weekly/Monthly | Ops Analytics |
| Inbound | % Qty Discrepancies | Sum discrepancy_qty / sum qty_ordered | SUM(discrepancy_qty)/SUM(qty_ordered) | inbound_parts | Weekly/Monthly | Quality |
| Inbound | Top 5 Delaying Suppliers | Suppliers with highest late volume | GROUP BY supplier_name ORDER BY SUM(CASE WHEN received_date>expected_date THEN qty_ordered END) DESC LIMIT 5 | inbound_parts | Monthly | Purchasing |
| Outbound | Fill Rate % | Shipped / Ordered | SUM(qty_shipped)/SUM(qty_ordered) | outbound_parts | Daily/Weekly | Customer Service |
| Outbound | OTIF % | Orders shipped on/before promise and in full | SUM(CASE WHEN otif_flag=1 THEN 1 ELSE 0 END)/COUNT(DISTINCT order_number) | outbound_parts | Weekly/Monthly | Logistics |
| Outbound | Backorder Rate % | Backordered qty / Ordered qty | SUM(backorder_qty)/SUM(qty_ordered) | outbound_parts | Weekly | Logistics |
| Outbound | Top 10 SKUs by Backorder | SKUs with highest backorders | GROUP BY part_number ORDER BY SUM(backorder_qty) DESC LIMIT 10 | outbound_parts | Weekly | Supply Planning |
| Inventory | Days of Supply | On-hand / Avg daily demand | on_hand_qty / NULLIF(avg_daily_demand,0) | inventory_snapshot, outbound_parts | Weekly | Supply Planning |
| Inventory | % Stock-out Days | Days with stockout flag / total days | SUM(stockout_flag)/COUNT(*) | inventory_snapshot | Weekly | Supply Planning |
| Inventory | Inventory Turns | Annualized COGS / Avg Inventory | (12 * SUM(monthly_issues_value)) / AVG(month_end_inventory_value) | inventory_snapshot (+ finance) | Monthly | Finance |
| Inventory | Aged Inventory Value >180d | Value of stock older than 180 days | SUM(CASE WHEN age_days>180 THEN on_hand_qty*unit_cost END) | inventory_snapshot (+ cost) | Monthly | Finance |
| WH Productivity | Lines Picked per Labor-Hour | Lines picked / labor hours | SUM(lines_picked)/SUM(labor_hours) | warehouse_productivity | Daily/Weekly | Operations |
| WH Productivity | Orders per Day | Total orders processed | SUM(orders_processed) | warehouse_productivity | Daily | Operations |
| WH Productivity | SLA Adherence % | Within SLA / total | AVG(sla_adherence_pct) | warehouse_productivity | Daily/Weekly | Operations |
| Employee | Picks per Person per Hour | Picks / hours worked | SUM(picks)/SUM(hours_worked) | employee_productivity | Daily/Weekly | Operations |
| Employee | Error Rate % | Errors / tasks | SUM(errors)/NULLIF(SUM(tasks_completed),0) | employee_productivity | Weekly | Quality |
| Employee | Overtime % | OT hours / total hours | SUM(overtime_hours)/SUM(hours_worked+overtime_hours) | employee_productivity | Weekly | HR |
