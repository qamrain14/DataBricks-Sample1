-- ============================================================
-- DEMO SCRIPT  : SET A — Data Changes  (44 statements)
-- Catalog      : workspace
-- Schema       : procurement_gold
-- Purpose      : Apply visible changes to gold tables so that
--                Power BI chart values shift on the next refresh.
-- Revert with  : demo_revert_set_b.sql  (SET B)
-- Run order    : Execute section by section; each block is one
--                UPDATE followed immediately by a verification SELECT.
-- ============================================================


-- ============================================================
-- SECTION 1 : fact_purchase_orders  (A01 – A06)
-- ============================================================

-- A01 · Raise po_value +30 % for first 5 APPROVED POs
-- Chart impact : Total Spend KPI · Spend-by-Vendor bar chart
-- ============================================================
UPDATE workspace.procurement_gold.fact_purchase_orders
SET    po_value = ROUND(po_value * 1.30, 2)
WHERE  po_id IN (
          SELECT po_id
          FROM   workspace.procurement_gold.fact_purchase_orders
          WHERE  po_status = 'APPROVED'
          ORDER  BY po_id ASC
          LIMIT  5
       );

SELECT po_id, vendor_id, po_value, po_status
FROM   workspace.procurement_gold.fact_purchase_orders
WHERE  po_status = 'APPROVED'
ORDER  BY po_id ASC
LIMIT  5;

-- A02 · Inflate total_amount +20 % on the first 5 POs overall
-- Chart impact : Total PO Amount KPI card
-- ============================================================
UPDATE workspace.procurement_gold.fact_purchase_orders
SET    total_amount = ROUND(total_amount * 1.20, 2)
WHERE  po_id IN (
          SELECT po_id
          FROM   workspace.procurement_gold.fact_purchase_orders
          ORDER  BY po_id ASC
          LIMIT  5
       );

SELECT po_id, total_amount
FROM   workspace.procurement_gold.fact_purchase_orders
ORDER  BY po_id ASC
LIMIT  5;

-- A03 · Flip 3 OPEN POs to CANCELLED
-- Chart impact : PO Status breakdown pie · OPEN vs CLOSED counts
-- ============================================================
UPDATE workspace.procurement_gold.fact_purchase_orders
SET    po_status = 'CANCELLED'
WHERE  po_id IN (
          SELECT po_id
          FROM   workspace.procurement_gold.fact_purchase_orders
          WHERE  po_status = 'OPEN'
          ORDER  BY po_id ASC
          LIMIT  3
       );

SELECT po_id, po_status
FROM   workspace.procurement_gold.fact_purchase_orders
WHERE  po_status = 'CANCELLED'
ORDER  BY po_id ASC
LIMIT  5;

-- A04 · Mark 5 NORMAL-priority POs as HIGH
-- Chart impact : Priority distribution bar chart
-- ============================================================
UPDATE workspace.procurement_gold.fact_purchase_orders
SET    priority = 'HIGH'
WHERE  po_id IN (
          SELECT po_id
          FROM   workspace.procurement_gold.fact_purchase_orders
          WHERE  priority = 'NORMAL'
          ORDER  BY po_id ASC
          LIMIT  5
       );

SELECT po_id, priority
FROM   workspace.procurement_gold.fact_purchase_orders
WHERE  priority = 'HIGH'
ORDER  BY po_id ASC
LIMIT  5;

-- A05 · Extend payment_terms to 90 days for first 5 POs
-- Chart impact : Average payment terms KPI
-- ============================================================
UPDATE workspace.procurement_gold.fact_purchase_orders
SET    payment_terms = 90
WHERE  po_id IN (
          SELECT po_id
          FROM   workspace.procurement_gold.fact_purchase_orders
          WHERE  payment_terms < 90
          ORDER  BY po_id ASC
          LIMIT  5
       );

SELECT po_id, payment_terms
FROM   workspace.procurement_gold.fact_purchase_orders
ORDER  BY po_id ASC
LIMIT  5;

-- A06 · Reclassify 3 STANDARD POs as EMERGENCY
-- Chart impact : PO Type breakdown · Emergency spend KPI
-- ============================================================
UPDATE workspace.procurement_gold.fact_purchase_orders
SET    po_type = 'EMERGENCY'
WHERE  po_id IN (
          SELECT po_id
          FROM   workspace.procurement_gold.fact_purchase_orders
          WHERE  po_type = 'STANDARD'
          ORDER  BY po_id ASC
          LIMIT  3
       );

SELECT po_id, po_type
FROM   workspace.procurement_gold.fact_purchase_orders
WHERE  po_type = 'EMERGENCY'
ORDER  BY po_id ASC
LIMIT  5;


-- ============================================================
-- SECTION 2 : fact_invoices  (A07 – A12)
-- ============================================================

-- A07 · Raise invoice_amount +25 % on first 5 UNPAID invoices
-- Chart impact : Outstanding invoice value KPI
-- ============================================================
UPDATE workspace.procurement_gold.fact_invoices
SET    invoice_amount = ROUND(invoice_amount * 1.25, 2)
WHERE  invoice_id IN (
          SELECT invoice_id
          FROM   workspace.procurement_gold.fact_invoices
          WHERE  payment_status IN ('UNPAID', 'PENDING')
          ORDER  BY invoice_id ASC
          LIMIT  5
       );

SELECT invoice_id, invoice_amount, payment_status
FROM   workspace.procurement_gold.fact_invoices
WHERE  payment_status IN ('UNPAID', 'PENDING')
ORDER  BY invoice_id ASC
LIMIT  5;

-- A08 · Push days_overdue to 95 for first 5 overdue invoices
-- Chart impact : Invoice aging waterfall · 60–90 d band fills
-- ============================================================
UPDATE workspace.procurement_gold.fact_invoices
SET    days_overdue = 95
WHERE  invoice_id IN (
          SELECT invoice_id
          FROM   workspace.procurement_gold.fact_invoices
          WHERE  days_overdue > 0
          ORDER  BY invoice_id ASC
          LIMIT  5
       );

SELECT invoice_id, days_overdue, aging_bucket
FROM   workspace.procurement_gold.fact_invoices
WHERE  days_overdue > 0
ORDER  BY invoice_id ASC
LIMIT  5;

-- A09 · Force aging_bucket to 'Over 90 days' for 3 invoices
-- Chart impact : Aging bucket distribution chart
-- ============================================================
UPDATE workspace.procurement_gold.fact_invoices
SET    aging_bucket = 'Over 90 days'
WHERE  invoice_id IN (
          SELECT invoice_id
          FROM   workspace.procurement_gold.fact_invoices
          WHERE  aging_bucket != 'Over 90 days'
          ORDER  BY invoice_id ASC
          LIMIT  3
       );

SELECT invoice_id, aging_bucket, days_overdue
FROM   workspace.procurement_gold.fact_invoices
ORDER  BY invoice_id ASC
LIMIT  5;

-- A10 · Mark 3 PENDING invoices as DISPUTED
-- Chart impact : Disputed invoices count · Disputed amount KPI
-- ============================================================
UPDATE workspace.procurement_gold.fact_invoices
SET    payment_status = 'DISPUTED'
WHERE  invoice_id IN (
          SELECT invoice_id
          FROM   workspace.procurement_gold.fact_invoices
          WHERE  payment_status = 'PENDING'
          ORDER  BY invoice_id ASC
          LIMIT  3
       );

SELECT invoice_id, payment_status
FROM   workspace.procurement_gold.fact_invoices
WHERE  payment_status = 'DISPUTED'
ORDER  BY invoice_id ASC
LIMIT  5;

-- A11 · Inflate net_amount +20 % for first 5 invoices
-- Chart impact : Net payables bar · AP turnover KPI
-- ============================================================
UPDATE workspace.procurement_gold.fact_invoices
SET    net_amount = ROUND(net_amount * 1.20, 2)
WHERE  invoice_id IN (
          SELECT invoice_id
          FROM   workspace.procurement_gold.fact_invoices
          ORDER  BY invoice_id ASC
          LIMIT  5
       );

SELECT invoice_id, net_amount
FROM   workspace.procurement_gold.fact_invoices
ORDER  BY invoice_id ASC
LIMIT  5;

-- A12 · Inflate gross_amount +15 % for first 5 invoices
-- Chart impact : Gross payables KPI · tax vs net split
-- ============================================================
UPDATE workspace.procurement_gold.fact_invoices
SET    gross_amount = ROUND(gross_amount * 1.15, 2)
WHERE  invoice_id IN (
          SELECT invoice_id
          FROM   workspace.procurement_gold.fact_invoices
          ORDER  BY invoice_id ASC
          LIMIT  5
       );

SELECT invoice_id, gross_amount, net_amount
FROM   workspace.procurement_gold.fact_invoices
ORDER  BY invoice_id ASC
LIMIT  5;


-- ============================================================
-- SECTION 3 : fact_contracts  (A13 – A17)
-- ============================================================

-- A13 · Raise contract_value +25 % on first 3 ACTIVE contracts
-- Chart impact : Total contract pipeline KPI
-- ============================================================
UPDATE workspace.procurement_gold.fact_contracts
SET    contract_value = ROUND(contract_value * 1.25, 2)
WHERE  contract_id IN (
          SELECT contract_id
          FROM   workspace.procurement_gold.fact_contracts
          WHERE  contract_status = 'ACTIVE'
          ORDER  BY contract_id ASC
          LIMIT  3
       );

SELECT contract_id, contract_value, contract_status
FROM   workspace.procurement_gold.fact_contracts
WHERE  contract_status = 'ACTIVE'
ORDER  BY contract_id ASC
LIMIT  3;

-- A14 · Raise revised_value +30 % on first 3 contracts
-- Chart impact : Contract cost-growth chart
-- ============================================================
UPDATE workspace.procurement_gold.fact_contracts
SET    revised_value = ROUND(revised_value * 1.30, 2)
WHERE  contract_id IN (
          SELECT contract_id
          FROM   workspace.procurement_gold.fact_contracts
          ORDER  BY contract_id ASC
          LIMIT  3
       );

SELECT contract_id, contract_value, revised_value
FROM   workspace.procurement_gold.fact_contracts
ORDER  BY contract_id ASC
LIMIT  3;

-- A15 · Set value_utilisation_pct = 97 for 3 contracts
-- Chart impact : Contract utilisation gauge · near-capacity alert
-- ============================================================
UPDATE workspace.procurement_gold.fact_contracts
SET    value_utilisation_pct = 97.0
WHERE  contract_id IN (
          SELECT contract_id
          FROM   workspace.procurement_gold.fact_contracts
          WHERE  value_utilisation_pct < 90
          ORDER  BY contract_id ASC
          LIMIT  3
       );

SELECT contract_id, value_utilisation_pct
FROM   workspace.procurement_gold.fact_contracts
ORDER  BY contract_id ASC
LIMIT  5;

-- A16 · Add 5 change orders to first 3 contracts
-- Chart impact : Change order frequency trend
-- ============================================================
UPDATE workspace.procurement_gold.fact_contracts
SET    change_order_count = change_order_count + 5
WHERE  contract_id IN (
          SELECT contract_id
          FROM   workspace.procurement_gold.fact_contracts
          ORDER  BY contract_id ASC
          LIMIT  3
       );

SELECT contract_id, change_order_count
FROM   workspace.procurement_gold.fact_contracts
ORDER  BY contract_id ASC
LIMIT  3;

-- A17 · Flag 2 ACTIVE contracts as AT_RISK
-- Chart impact : Contract status breakdown · risk flag count
-- ============================================================
UPDATE workspace.procurement_gold.fact_contracts
SET    contract_status = 'AT_RISK'
WHERE  contract_id IN (
          SELECT contract_id
          FROM   workspace.procurement_gold.fact_contracts
          WHERE  contract_status = 'ACTIVE'
          ORDER  BY contract_id ASC
          LIMIT  2
       );

SELECT contract_id, contract_status
FROM   workspace.procurement_gold.fact_contracts
WHERE  contract_status = 'AT_RISK'
ORDER  BY contract_id ASC
LIMIT  5;


-- ============================================================
-- SECTION 4 : fact_project_costs  (A18 – A22)
-- ============================================================

-- A18 · Raise actual_amount +40 % for first 5 cost lines
-- Chart impact : Budget vs Actual bar · CPI drops
-- ============================================================
UPDATE workspace.procurement_gold.fact_project_costs
SET    actual_amount = ROUND(actual_amount * 1.40, 2)
WHERE  budget_id IN (
          SELECT budget_id
          FROM   workspace.procurement_gold.fact_project_costs
          ORDER  BY budget_id ASC
          LIMIT  5
       );

SELECT budget_id, project_id, actual_amount, budget_amount
FROM   workspace.procurement_gold.fact_project_costs
ORDER  BY budget_id ASC
LIMIT  5;

-- A19 · Widen negative variance by –75 000 on first 5 lines
-- Chart impact : Variance waterfall turns more negative
-- ============================================================
UPDATE workspace.procurement_gold.fact_project_costs
SET    variance_amount = variance_amount - 75000
WHERE  budget_id IN (
          SELECT budget_id
          FROM   workspace.procurement_gold.fact_project_costs
          ORDER  BY budget_id ASC
          LIMIT  5
       );

SELECT budget_id, variance_amount, variance_pct
FROM   workspace.procurement_gold.fact_project_costs
ORDER  BY budget_id ASC
LIMIT  5;

-- A20 · Mark first 3 cost lines as OVERSPENT
-- Chart impact : Budget status KPI · OVERSPENT count badge
-- ============================================================
UPDATE workspace.procurement_gold.fact_project_costs
SET    budget_status = 'OVERSPENT'
WHERE  budget_id IN (
          SELECT budget_id
          FROM   workspace.procurement_gold.fact_project_costs
          WHERE  budget_status != 'OVERSPENT'
          ORDER  BY budget_id ASC
          LIMIT  3
       );

SELECT budget_id, budget_status
FROM   workspace.procurement_gold.fact_project_costs
WHERE  budget_status = 'OVERSPENT'
ORDER  BY budget_id ASC
LIMIT  5;

-- A21 · Set utilization_pct = 115 for first 3 cost lines
-- Chart impact : Over-utilisation alert · gauge crosses 100 %
-- ============================================================
UPDATE workspace.procurement_gold.fact_project_costs
SET    utilization_pct = 115.0
WHERE  budget_id IN (
          SELECT budget_id
          FROM   workspace.procurement_gold.fact_project_costs
          WHERE  utilization_pct < 100
          ORDER  BY budget_id ASC
          LIMIT  3
       );

SELECT budget_id, utilization_pct, budget_status
FROM   workspace.procurement_gold.fact_project_costs
ORDER  BY budget_id ASC
LIMIT  5;

-- A22 · Halve remaining_amount for first 5 positive cost lines
-- Chart impact : Remaining budget waterfall drops sharply
-- ============================================================
UPDATE workspace.procurement_gold.fact_project_costs
SET    remaining_amount = ROUND(remaining_amount * 0.50, 2)
WHERE  budget_id IN (
          SELECT budget_id
          FROM   workspace.procurement_gold.fact_project_costs
          WHERE  remaining_amount > 0
          ORDER  BY budget_id ASC
          LIMIT  5
       );

SELECT budget_id, remaining_amount, budget_amount
FROM   workspace.procurement_gold.fact_project_costs
ORDER  BY budget_id ASC
LIMIT  5;


-- ============================================================
-- SECTION 5 : fact_goods_receipts  (A23 – A27)
-- ============================================================

-- A23 · Set rejection_rate = 0.40 for first 5 low-rejection receipts
-- Chart impact : Defect rate spike on quality chart
-- ============================================================
UPDATE workspace.procurement_gold.fact_goods_receipts
SET    rejection_rate = 0.40
WHERE  grn_id IN (
          SELECT grn_id
          FROM   workspace.procurement_gold.fact_goods_receipts
          WHERE  rejection_rate < 0.20
          ORDER  BY grn_id ASC
          LIMIT  5
       );

SELECT grn_id, rejection_rate, acceptance_rate
FROM   workspace.procurement_gold.fact_goods_receipts
ORDER  BY grn_id ASC
LIMIT  5;

-- A24 · Add 50 rejected units to first 5 receipts
-- Chart impact : Rejection volume bar chart increases
-- ============================================================
UPDATE workspace.procurement_gold.fact_goods_receipts
SET    quantity_rejected = quantity_rejected + 50
WHERE  grn_id IN (
          SELECT grn_id
          FROM   workspace.procurement_gold.fact_goods_receipts
          ORDER  BY grn_id ASC
          LIMIT  5
       );

SELECT grn_id, quantity_received, quantity_rejected
FROM   workspace.procurement_gold.fact_goods_receipts
ORDER  BY grn_id ASC
LIMIT  5;

-- A25 · Flip 3 RECEIVED GRNs to REJECTED
-- Chart impact : GRN status donut · Acceptance rate KPI drops
-- ============================================================
UPDATE workspace.procurement_gold.fact_goods_receipts
SET    grn_status = 'REJECTED'
WHERE  grn_id IN (
          SELECT grn_id
          FROM   workspace.procurement_gold.fact_goods_receipts
          WHERE  grn_status = 'RECEIVED'
          ORDER  BY grn_id ASC
          LIMIT  3
       );

SELECT grn_id, grn_status
FROM   workspace.procurement_gold.fact_goods_receipts
WHERE  grn_status = 'REJECTED'
ORDER  BY grn_id ASC
LIMIT  5;

-- A26 · Drop acceptance_rate to 0.60 for first 5 high-acceptance receipts
-- Chart impact : Quality tier downgrade · avg acceptance KPI
-- ============================================================
UPDATE workspace.procurement_gold.fact_goods_receipts
SET    acceptance_rate = 0.60
WHERE  grn_id IN (
          SELECT grn_id
          FROM   workspace.procurement_gold.fact_goods_receipts
          WHERE  acceptance_rate > 0.80
          ORDER  BY grn_id ASC
          LIMIT  5
       );

SELECT grn_id, acceptance_rate, rejection_rate
FROM   workspace.procurement_gold.fact_goods_receipts
ORDER  BY grn_id ASC
LIMIT  5;

-- A27 · Raise receipt_value +25 % for first 5 GRNs
-- Chart impact : Receipt value trend line shifts upward
-- ============================================================
UPDATE workspace.procurement_gold.fact_goods_receipts
SET    receipt_value = ROUND(receipt_value * 1.25, 2)
WHERE  grn_id IN (
          SELECT grn_id
          FROM   workspace.procurement_gold.fact_goods_receipts
          ORDER  BY grn_id ASC
          LIMIT  5
       );

SELECT grn_id, receipt_value
FROM   workspace.procurement_gold.fact_goods_receipts
ORDER  BY grn_id ASC
LIMIT  5;


-- ============================================================
-- SECTION 6 : fact_sales  (A28 – A31)
-- ============================================================

-- A28 · Raise revenue +50 % for first 5 COMPLETED sales orders
-- Chart impact : Total Revenue KPI jumps visibly
-- ============================================================
UPDATE workspace.procurement_gold.fact_sales
SET    revenue = ROUND(revenue * 1.50, 2)
WHERE  sales_order_id IN (
          SELECT sales_order_id
          FROM   workspace.procurement_gold.fact_sales
          WHERE  order_status = 'COMPLETED'
          ORDER  BY sales_order_id ASC
          LIMIT  5
       );

SELECT sales_order_id, revenue, order_status
FROM   workspace.procurement_gold.fact_sales
WHERE  order_status = 'COMPLETED'
ORDER  BY sales_order_id ASC
LIMIT  5;

-- A29 · Cut margin_pct by 15 pp for first 5 sales orders
-- Chart impact : Gross margin chart drops · compression visible
-- ============================================================
UPDATE workspace.procurement_gold.fact_sales
SET    margin_pct = margin_pct - 15.0
WHERE  sales_order_id IN (
          SELECT sales_order_id
          FROM   workspace.procurement_gold.fact_sales
          ORDER  BY sales_order_id ASC
          LIMIT  5
       );

SELECT sales_order_id, margin_pct, revenue, cogs
FROM   workspace.procurement_gold.fact_sales
ORDER  BY sales_order_id ASC
LIMIT  5;

-- A30 · Cancel 3 PENDING sales orders
-- Chart impact : Fulfilled order count drops · cancellation rate rises
-- ============================================================
UPDATE workspace.procurement_gold.fact_sales
SET    order_status = 'CANCELLED'
WHERE  sales_order_id IN (
          SELECT sales_order_id
          FROM   workspace.procurement_gold.fact_sales
          WHERE  order_status = 'PENDING'
          ORDER  BY sales_order_id ASC
          LIMIT  3
       );

SELECT sales_order_id, order_status
FROM   workspace.procurement_gold.fact_sales
WHERE  order_status = 'CANCELLED'
ORDER  BY sales_order_id ASC
LIMIT  5;

-- A31 · Raise unit_price +30 % for first 5 sales orders
-- Chart impact : Average unit price trend shifts upward
-- ============================================================
UPDATE workspace.procurement_gold.fact_sales
SET    unit_price = ROUND(unit_price * 1.30, 2)
WHERE  sales_order_id IN (
          SELECT sales_order_id
          FROM   workspace.procurement_gold.fact_sales
          ORDER  BY sales_order_id ASC
          LIMIT  5
       );

SELECT sales_order_id, unit_price, revenue
FROM   workspace.procurement_gold.fact_sales
ORDER  BY sales_order_id ASC
LIMIT  5;


-- ============================================================
-- SECTION 7 : fact_vendor_performance  (A32 – A34)
-- ============================================================

-- A32 · Drop overall_score to 35 for first 3 high-scoring evaluations
-- Chart impact : Vendor scorecard drops into RED zone
-- ============================================================
UPDATE workspace.procurement_gold.fact_vendor_performance
SET    overall_score = 35.0
WHERE  performance_id IN (
          SELECT performance_id
          FROM   workspace.procurement_gold.fact_vendor_performance
          WHERE  overall_score > 60
          ORDER  BY performance_id ASC
          LIMIT  3
       );

SELECT performance_id, vendor_id, overall_score, recommendation
FROM   workspace.procurement_gold.fact_vendor_performance
ORDER  BY performance_id ASC
LIMIT  5;

-- A33 · Set recommendation = 'REVIEW' for first 3 APPROVED evaluations
-- Chart impact : Recommended vendor count KPI decreases
-- ============================================================
UPDATE workspace.procurement_gold.fact_vendor_performance
SET    recommendation = 'REVIEW'
WHERE  performance_id IN (
          SELECT performance_id
          FROM   workspace.procurement_gold.fact_vendor_performance
          WHERE  recommendation = 'APPROVED'
          ORDER  BY performance_id ASC
          LIMIT  3
       );

SELECT performance_id, vendor_id, recommendation
FROM   workspace.procurement_gold.fact_vendor_performance
WHERE  recommendation IN ('REVIEW', 'APPROVED')
ORDER  BY performance_id ASC
LIMIT  5;

-- A34 · Drop quality_score to 30 for first 3 high-scoring evaluations
-- Chart impact : Quality dimension bar / radar chart drops
-- ============================================================
UPDATE workspace.procurement_gold.fact_vendor_performance
SET    quality_score = 30.0
WHERE  performance_id IN (
          SELECT performance_id
          FROM   workspace.procurement_gold.fact_vendor_performance
          WHERE  quality_score > 60
          ORDER  BY performance_id ASC
          LIMIT  3
       );

SELECT performance_id, vendor_id, quality_score, delivery_score
FROM   workspace.procurement_gold.fact_vendor_performance
ORDER  BY performance_id ASC
LIMIT  5;


-- ============================================================
-- SECTION 8 : dim_vendor  (A35 – A38)
-- ============================================================

-- A35 · Drop performance_rating to 1.5 for first 3 top-rated vendors
-- Chart impact : Avg vendor rating KPI card drops
-- ============================================================
UPDATE workspace.procurement_gold.dim_vendor
SET    performance_rating = 1.5
WHERE  vendor_id IN (
          SELECT vendor_id
          FROM   workspace.procurement_gold.dim_vendor
          WHERE  performance_rating >= 4.0
            AND  status = 'ACTIVE'
          ORDER  BY vendor_id ASC
          LIMIT  3
       );

SELECT vendor_id, vendor_name, performance_rating, risk_category
FROM   workspace.procurement_gold.dim_vendor
ORDER  BY vendor_id ASC
LIMIT  5;

-- A36 · Reclassify 3 LOW-risk vendors as HIGH risk
-- Chart impact : Vendor risk category pie shifts
-- ============================================================
UPDATE workspace.procurement_gold.dim_vendor
SET    risk_category = 'HIGH'
WHERE  vendor_id IN (
          SELECT vendor_id
          FROM   workspace.procurement_gold.dim_vendor
          WHERE  risk_category = 'LOW'
          ORDER  BY vendor_id ASC
          LIMIT  3
       );

SELECT vendor_id, vendor_name, risk_category, tier
FROM   workspace.procurement_gold.dim_vendor
WHERE  risk_category = 'HIGH'
ORDER  BY vendor_id ASC
LIMIT  5;

-- A37 · Downgrade 5 Tier-A/B vendors to Tier C
-- Chart impact : Vendor tier breakdown chart shifts
-- ============================================================
UPDATE workspace.procurement_gold.dim_vendor
SET    tier = 'C'
WHERE  vendor_id IN (
          SELECT vendor_id
          FROM   workspace.procurement_gold.dim_vendor
          WHERE  tier IN ('A', 'B')
          ORDER  BY vendor_id ASC
          LIMIT  5
       );

SELECT vendor_id, vendor_name, tier, status
FROM   workspace.procurement_gold.dim_vendor
ORDER  BY vendor_id ASC
LIMIT  5;

-- A38 · Suspend 2 ACTIVE vendors
-- Chart impact : Active vendor count KPI drops by 2
-- ============================================================
UPDATE workspace.procurement_gold.dim_vendor
SET    status = 'SUSPENDED'
WHERE  vendor_id IN (
          SELECT vendor_id
          FROM   workspace.procurement_gold.dim_vendor
          WHERE  status = 'ACTIVE'
          ORDER  BY vendor_id ASC
          LIMIT  2
       );

SELECT vendor_id, vendor_name, status
FROM   workspace.procurement_gold.dim_vendor
WHERE  status = 'SUSPENDED'
ORDER  BY vendor_id ASC
LIMIT  5;


-- ============================================================
-- SECTION 9 : dim_project  (A39 – A41)
-- ============================================================

-- A39 · Put 3 ACTIVE projects ON_HOLD
-- Chart impact : Active project count drops · On-Hold count rises
-- ============================================================
UPDATE workspace.procurement_gold.dim_project
SET    project_status = 'ON_HOLD'
WHERE  project_id IN (
          SELECT project_id
          FROM   workspace.procurement_gold.dim_project
          WHERE  project_status = 'ACTIVE'
          ORDER  BY project_id ASC
          LIMIT  3
       );

SELECT project_id, project_name, project_status, risk_level
FROM   workspace.procurement_gold.dim_project
WHERE  project_status = 'ON_HOLD'
ORDER  BY project_id ASC
LIMIT  5;

-- A40 · Escalate 3 LOW-risk projects to CRITICAL
-- Chart impact : Risk distribution chart shifts to critical band
-- ============================================================
UPDATE workspace.procurement_gold.dim_project
SET    risk_level = 'CRITICAL'
WHERE  project_id IN (
          SELECT project_id
          FROM   workspace.procurement_gold.dim_project
          WHERE  risk_level = 'LOW'
          ORDER  BY project_id ASC
          LIMIT  3
       );

SELECT project_id, project_name, risk_level
FROM   workspace.procurement_gold.dim_project
WHERE  risk_level = 'CRITICAL'
ORDER  BY project_id ASC
LIMIT  5;

-- A41 · Increase approved_budget_usd +35 % for first 3 projects
-- Chart impact : Total approved budget KPI jumps
-- ============================================================
UPDATE workspace.procurement_gold.dim_project
SET    approved_budget_usd = ROUND(approved_budget_usd * 1.35, 2)
WHERE  project_id IN (
          SELECT project_id
          FROM   workspace.procurement_gold.dim_project
          ORDER  BY project_id ASC
          LIMIT  3
       );

SELECT project_id, project_name, approved_budget_usd
FROM   workspace.procurement_gold.dim_project
ORDER  BY project_id ASC
LIMIT  3;


-- ============================================================
-- SECTION 10 : dim_material  (A42 – A44)
-- ============================================================

-- A42 · Raise unit_price +45 % for first 5 active materials
-- Chart impact : Material cost trend chart shifts upward
-- ============================================================
UPDATE workspace.procurement_gold.dim_material
SET    unit_price = ROUND(unit_price * 1.45, 2)
WHERE  material_id IN (
          SELECT material_id
          FROM   workspace.procurement_gold.dim_material
          WHERE  is_active = TRUE
          ORDER  BY material_id ASC
          LIMIT  5
       );

SELECT material_id, material_name, unit_price, category
FROM   workspace.procurement_gold.dim_material
ORDER  BY material_id ASC
LIMIT  5;

-- A43 · Extend lead_time_days to 60 for first 5 short-lead materials
-- Chart impact : Lead time distribution chart shifts right
-- ============================================================
UPDATE workspace.procurement_gold.dim_material
SET    lead_time_days = 60
WHERE  material_id IN (
          SELECT material_id
          FROM   workspace.procurement_gold.dim_material
          WHERE  lead_time_days < 30
          ORDER  BY material_id ASC
          LIMIT  5
       );

SELECT material_id, material_name, lead_time_days
FROM   workspace.procurement_gold.dim_material
ORDER  BY material_id ASC
LIMIT  5;

-- A44 · Deactivate first 3 active materials
-- Chart impact : Active material count KPI drops
-- ============================================================
UPDATE workspace.procurement_gold.dim_material
SET    is_active = FALSE
WHERE  material_id IN (
          SELECT material_id
          FROM   workspace.procurement_gold.dim_material
          WHERE  is_active = TRUE
          ORDER  BY material_id ASC
          LIMIT  3
       );

SELECT material_id, material_name, is_active
FROM   workspace.procurement_gold.dim_material
WHERE  is_active = FALSE
ORDER  BY material_id ASC
LIMIT  5;

-- ============================================================
-- END OF SET A
-- After running this script, refresh Power BI Desktop to see
-- chart values change, then run demo_revert_set_b.sql to restore.
-- ============================================================
