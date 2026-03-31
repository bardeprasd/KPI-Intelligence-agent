# Data Model

## Purpose
The KPI intelligence agent is modeled as a lightweight warehouse analytics mart. The conceptual data model separates measurable operational events into fact tables and reusable business descriptors into dimensions.

This design supports:
- consistent KPI definitions
- cross-domain analysis
- period-based filtering
- warehouse and part-level slicing
- future BI or SQL warehouse implementation

## Conceptual Fact Tables

### 1. `fact_inbound_receipts`
**Business event:** receipt of inbound parts against purchase orders  
**Grain:** one purchase-order line / receipt line per `po_number` + `part_number` + `received_date`

| Column | Type | Description |
|---|---|---|
| po_number | string | Purchase order identifier |
| supplier_id | string | Supplier key |
| supplier_name | string | Supplier descriptive attribute |
| part_number | string | Part / SKU key |
| expected_date | date | Expected receipt date |
| received_date | date | Actual receipt date |
| qty_ordered | integer | Ordered quantity |
| qty_received | integer | Received quantity |
| inbound_lead_time_days | integer | Actual receipt lead time |
| discrepancy_qty | integer | Ordered less received |

### 2. `fact_outbound_orders`
**Business event:** shipment fulfillment against customer orders  
**Grain:** one outbound order line per `order_number` + `part_number` + `shipped_date`

| Column | Type | Description |
|---|---|---|
| order_number | string | Sales order identifier |
| customer_id | string | Customer key |
| customer_name | string | Customer descriptive attribute |
| part_number | string | Part / SKU key |
| order_date | date | Original order date |
| promise_date | date | Promised ship/delivery date |
| shipped_date | date | Actual shipped date |
| qty_ordered | integer | Ordered quantity |
| qty_shipped | integer | Shipped quantity |
| backorder_qty | integer | Ordered not shipped |
| otif_flag | integer / boolean | On-time in-full outcome |
| fill_rate | decimal | Upstream provided fill rate |

### 3. `fact_inventory_snapshot`
**Business event:** inventory state snapshot at a point in time  
**Grain:** one warehouse-location-part snapshot per `snapshot_date` + `warehouse_id` + `location` + `part_number`

| Column | Type | Description |
|---|---|---|
| snapshot_date | date | Snapshot date |
| warehouse_id | string | Warehouse key |
| warehouse_name | string | Warehouse descriptive attribute |
| location | string | Bin or location identifier |
| part_number | string | Part / SKU key |
| sku_family | string | Product family / category |
| on_hand_qty | integer | Physical stock |
| available_qty | integer | Available to promise |
| safety_stock | integer | Target safety buffer |
| reorder_point | integer | Reorder trigger |
| days_of_supply | integer | Operational days of supply |
| stockout_flag | integer / boolean | Stockout indicator |
| age_days | integer | Inventory age in days |

### 4. `fact_warehouse_productivity`
**Business event:** warehouse operational productivity by date and shift  
**Grain:** one warehouse-day-shift productivity aggregate per `date` + `warehouse_id` + `shift`

| Column | Type | Description |
|---|---|---|
| date | date | Activity date |
| warehouse_id | string | Warehouse key |
| shift | string | Shift key |
| lines_picked | integer | Picked lines |
| lines_packed | integer | Packed lines |
| orders_processed | integer | Processed orders |
| labor_hours | decimal | Labor hours consumed |
| picks_per_hour | decimal | Upstream productivity metric |
| touches_per_order | decimal | Handling touches |
| equipment_utilization_pct | decimal | Equipment utilization |
| sla_adherence_pct | decimal | SLA adherence |

### 5. `fact_employee_productivity`
**Business event:** employee productivity by date and shift  
**Grain:** one employee-day record per `date` + `employee_id`

| Column | Type | Description |
|---|---|---|
| date | date | Activity date |
| employee_id | string | Employee key |
| role | string | Employee role |
| warehouse_id | string | Warehouse key |
| shift | string | Shift key |
| tasks_completed | integer | Tasks completed |
| picks | integer | Picks completed |
| hours_worked | decimal | Regular hours |
| picks_per_hour | decimal | Upstream employee productivity metric |
| errors | integer | Quality / processing errors |
| rework | integer | Rework tasks |
| overtime_hours | decimal | Overtime hours |

## Conceptual Dimension Tables

### `dim_date`
Shared calendar dimension used for monthly, weekly, and daily aggregation.

| Column | Description |
|---|---|
| date_key | Surrogate YYYYMMDD key |
| full_date | Calendar date |
| year | Calendar year |
| quarter | Calendar quarter |
| month | Calendar month number |
| month_name | Month name |
| week_of_year | ISO week |
| day_of_week | Weekday |
| fiscal_period | Optional future extension |

### `dim_warehouse`
| Column | Description |
|---|---|
| warehouse_id | Natural key |
| warehouse_name | Display name |
| region | Optional future attribute |
| warehouse_type | Optional future attribute |

### `dim_part`
| Column | Description |
|---|---|
| part_number | Natural key |
| sku_family | Product family |
| part_description | Future attribute |
| lifecycle_status | Future attribute |

### `dim_supplier`
| Column | Description |
|---|---|
| supplier_id | Natural key |
| supplier_name | Supplier display name |
| supplier_tier | Future attribute |
| country | Future attribute |

### `dim_customer`
| Column | Description |
|---|---|
| customer_id | Natural key |
| customer_name | Customer display name |
| segment | Future attribute |
| region | Future attribute |

### `dim_employee`
| Column | Description |
|---|---|
| employee_id | Natural key |
| role | Employee role |
| warehouse_id | Home warehouse |
| active_flag | Future attribute |

### `dim_shift`
| Column | Description |
|---|---|
| shift | Natural key such as Day / Night |
| shift_start_time | Future attribute |
| shift_end_time | Future attribute |

## Relationships

### Core relationship map
- `part_number` links inbound, outbound, and inventory facts
- `warehouse_id` links inventory, warehouse productivity, and employee productivity
- `supplier_id` joins inbound receipts to supplier dimension
- `customer_id` joins outbound orders to customer dimension
- `employee_id` joins employee productivity to employee dimension
- date columns join each fact to `dim_date`
- `shift` joins warehouse and employee productivity to `dim_shift`

### Analytical implications
1. **Inbound to Outbound:** delayed inbound parts can be compared with outbound service issues for the same part numbers.
2. **Inventory to Outbound:** stockout and safety-stock risk can be tied to backorders and low fill rate.
3. **Warehouse to Employee Productivity:** overall warehouse throughput can be decomposed into labor performance, shift patterns, and overtime pressure.
4. **Part and Warehouse slicing:** leadership can identify whether performance issues are broad-based or isolated to specific nodes.

## Table Grain Notes
- Inbound and outbound are transactional facts.
- Inventory is a periodic snapshot fact.
- Warehouse productivity is an operational aggregate fact.
- Employee productivity is a semi-additive workforce performance fact.

This matters because:
- inventory quantities should not be naively summed across time without date awareness
- operational rates should be recomputed from numerators and denominators when possible
- averages should be explicit about whether they are row-level or weighted

## Key Columns and Business Keys
| Table | Primary business key |
|---|---|
| fact_inbound_receipts | po_number + part_number + received_date |
| fact_outbound_orders | order_number + part_number + shipped_date |
| fact_inventory_snapshot | snapshot_date + warehouse_id + location + part_number |
| fact_warehouse_productivity | date + warehouse_id + shift |
| fact_employee_productivity | date + employee_id |

## Sample Data Rows

The assignment asks for sample data in addition to table definitions. The following rows are representative examples from the provided CSV extracts.

### `fact_inbound_receipts`

| po_number | supplier_id | supplier_name | part_number | expected_date | received_date | qty_ordered | qty_received | inbound_lead_time_days | discrepancy_qty |
|---|---|---|---|---|---|---:|---:|---:|---:|
| PO10000 | SUP-003 | Supplier C | CL-7781 | 2026-01-15 | 2026-01-22 | 398 | 384 | 7 | 14 |
| PO10001 | SUP-003 | Supplier C | MN-5544 | 2024-11-20 | 2024-12-01 | 121 | 101 | 11 | 20 |

### `fact_outbound_orders`

| order_number | customer_id | customer_name | part_number | order_date | promise_date | shipped_date | qty_ordered | qty_shipped | backorder_qty | otif_flag | fill_rate |
|---|---|---|---|---|---|---|---:|---:|---:|---:|---:|
| SO20000 | CUST-003 | Customer Z | CL-7781 | 2025-05-12 | 2025-05-14 | 2025-05-13 | 25 | 25 | 0 | 1 | 1.000 |
| SO20001 | CUST-003 | Customer Z | ZX-9910 | 2025-12-03 | 2025-12-06 | 2025-12-08 | 24 | 15 | 9 | 0 | 0.625 |

### `fact_inventory_snapshot`

| snapshot_date | warehouse_id | warehouse_name | location | part_number | sku_family | on_hand_qty | available_qty | safety_stock | reorder_point | days_of_supply | stockout_flag | age_days |
|---|---|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|
| 2025-10-01 | WH-03 | Warehouse WH-03 | A5-03 | ZX-9910 | ZX | 316 | 260 | 156 | 227 | 88 | 1 | 239 |
| 2025-04-27 | WH-01 | Warehouse WH-01 | A2-05 | AX-4312 | AX | 390 | 383 | 166 | 197 | 68 | 0 | 202 |

## Sample Schema Descriptions

### Example: `fact_inbound_receipts`
Used to measure supplier timeliness, discrepancy rates, and inbound volume. Most period reporting should filter on `received_date` because the business event is completion of receipt.

### Example: `fact_inventory_snapshot`
Used to measure health and risk, including days of supply, stockout exposure, and aging. For monthly leadership reporting, the final snapshot in the reporting period is a useful anchor, while all snapshots in the month can be used for exposure rates.

### Example: `fact_warehouse_productivity`
Used to measure throughput efficiency. Weighted rates should use actual line and labor totals rather than averaging precomputed row-level rates.

## Recommended Physical Evolution
For production use, the next step would be:
1. raw landing tables
2. validated staging tables
3. curated fact and dimension marts
4. KPI aggregate marts
5. one-pager publishing layer

This assignment implementation keeps those layers logical rather than physically persisted, while preserving the same design intent.
