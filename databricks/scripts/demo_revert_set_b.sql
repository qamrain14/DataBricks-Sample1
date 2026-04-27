-- ============================================================
-- DEMO SCRIPT  : SET B — Revert Changes  (44 statements)
-- Catalog      : workspace
-- Schema       : procurement_gold
-- Purpose      : Restore every value changed by SET A so that
--                Power BI charts return to baseline on refresh.
-- Apply AFTER  : demo_upsert_set_a.sql  (SET A)
-- ============================================================


-- ============================================================
-- SECTION 1 : fact_purchase_orders  (B01 – B06)
-- ============================================================

-- B01 · Revert po_value: ÷ 1.30 for same 5 APPROVED POs
-- ============================================================
UPDATE workspace.procurement_gold.fact_purchase_orders
SET    po_value = ROUND(po_value / 1.30, 2)
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

-- B02 · Revert total_amount: ÷ 1.20 for first 5 POs
-- ============================================================
UPDATE workspace.procurement_gold.fact_purchase_orders
SET    total_amount = ROUND(total_amount / 1.20, 2)
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

-- B03 · Restore 3 CANCELLED POs back to OPEN
-- ============================================================
UPDATE workspace.procurement_gold.fact_purchase_orders
SET    po_status = 'OPEN'
WHERE  po_id IN (
          SELECT po_id
          FROM   workspace.procurement_gold.fact_purchase_orders
          WHERE  po_status = 'CANCELLED'
          ORDER  BY po_id ASC
          LIMIT  3
       );

SELECT po_id, po_status
FROM   workspace.procurement_gold.fact_purchase_orders
WHERE  po_status = 'OPEN'
ORDER  BY po_id ASC
LIMIT  5;

-- B04 · Restore 5 HIGH-priority POs back to NORMAL
-- ============================================================
UPDATE workspace.procurement_gold.fact_purchase_orders
SET    priority = 'NORMAL'
WHERE  po_id IN (
          SELECT po_id
          FROM   workspace.procurement_gold.fact_purchase_orders
          WHERE  priority = 'HIGH'
          ORDER  BY po_id ASC
          LIMIT  5
       );

SELECT po_id, priority
FROM   workspace.procurement_gold.fact_purchase_orders
ORDER  BY po_id ASC
LIMIT  5;

-- B05 · Restore payment_terms to 30 for same 5 POs  (SET A set them to 90)
-- ============================================================
UPDATE workspace.procurement_gold.fact_purchase_orders
SET    payment_terms = 30
WHERE  po_id IN (
          SELECT po_id
          FROM   workspace.procurement_gold.fact_purchase_orders
          WHERE  payment_terms = 90
          ORDER  BY po_id ASC
          LIMIT  5
       );

SELECT po_id, payment_terms
FROM   workspace.procurement_gold.fact_purchase_orders
ORDER  BY po_id ASC
LIMIT  5;

-- B06 · Restore 3 EMERGENCY POs back to STANDARD
-- ============================================================
UPDATE workspace.procurement_gold.fact_purchase_orders
SET    po_type = 'STANDARD'
WHERE  po_id IN (
          SELECT po_id
          FROM   workspace.procurement_gold.fact_purchase_orders
          WHERE  po_type = 'EMERGENCY'
          ORDER  BY po_id ASC
          LIMIT  3
       );

SELECT po_id, po_type
FROM   workspace.procurement_gold.fact_purchase_orders
ORDER  BY po_id ASC
LIMIT  5;


-- ============================================================
-- SECTION 2 : fact_invoices  (B07 – B12)
-- ============================================================

-- B07 · Revert invoice_amount: ÷ 1.25 for same 5 UNPAID invoices
-- ============================================================
UPDATE workspace.procurement_gold.fact_invoices
SET    invoice_amount = ROUND(invoice_amount / 1.25, 2)
WHERE  invoice_id IN (
          SELECT invoice_id
          FROM   workspace.procurement_gold.fact_invoices
          WHERE  payment_status IN ('UNPAID', 'PENDING')
          ORDER  BY invoice_id ASC
          LIMIT  5
       );

SELECT invoice_id, invoice_amount, payment_status
FROM   workspace.procurement_gold.fact_invoices
ORDER  BY invoice_id ASC
LIMIT  5;

-- B08 · Restore days_overdue to 30 for invoices SET A pushed to 95
-- NOTE: 30 is a representative pre-change baseline
-- ============================================================
UPDATE workspace.procurement_gold.fact_invoices
SET    days_overdue = 30
WHERE  invoice_id IN (
          SELECT invoice_id
          FROM   workspace.procurement_gold.fact_invoices
          WHERE  days_overdue = 95
          ORDER  BY invoice_id ASC
          LIMIT  5
       );

SELECT invoice_id, days_overdue, aging_bucket
FROM   workspace.procurement_gold.fact_invoices
ORDER  BY invoice_id ASC
LIMIT  5;

-- B09 · Restore aging_bucket from 'Over 90 days' to '30-60 days'
-- ============================================================
UPDATE workspace.procurement_gold.fact_invoices
SET    aging_bucket = '30-60 days'
WHERE  invoice_id IN (
          SELECT invoice_id
          FROM   workspace.procurement_gold.fact_invoices
          WHERE  aging_bucket = 'Over 90 days'
          ORDER  BY invoice_id ASC
          LIMIT  3
       );

SELECT invoice_id, aging_bucket
FROM   workspace.procurement_gold.fact_invoices
ORDER  BY invoice_id ASC
LIMIT  5;

-- B10 · Restore 3 DISPUTED invoices back to PENDING
-- ============================================================
UPDATE workspace.procurement_gold.fact_invoices
SET    payment_status = 'PENDING'
WHERE  invoice_id IN (
          SELECT invoice_id
          FROM   workspace.procurement_gold.fact_invoices
          WHERE  payment_status = 'DISPUTED'
          ORDER  BY invoice_id ASC
          LIMIT  3
       );

SELECT invoice_id, payment_status
FROM   workspace.procurement_gold.fact_invoices
ORDER  BY invoice_id ASC
LIMIT  5;

-- B11 · Revert net_amount: ÷ 1.20 for first 5 invoices
-- ============================================================
UPDATE workspace.procurement_gold.fact_invoices
SET    net_amount = ROUND(net_amount / 1.20, 2)
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

-- B12 · Revert gross_amount: ÷ 1.15 for first 5 invoices
-- ============================================================
UPDATE workspace.procurement_gold.fact_invoices
SET    gross_amount = ROUND(gross_amount / 1.15, 2)
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
-- SECTION 3 : fact_contracts  (B13 – B17)
-- ============================================================

-- B13 · Revert contract_value: ÷ 1.25 for same 3 ACTIVE contracts
-- ============================================================
UPDATE workspace.procurement_gold.fact_contracts
SET    contract_value = ROUND(contract_value / 1.25, 2)
WHERE  contract_id IN (
          SELECT contract_id
          FROM   workspace.procurement_gold.fact_contracts
          WHERE  contract_status = 'ACTIVE'
          ORDER  BY contract_id ASC
          LIMIT  3
       );

SELECT contract_id, contract_value, contract_status
FROM   workspace.procurement_gold.fact_contracts
ORDER  BY contract_id ASC
LIMIT  3;

-- B14 · Revert revised_value: ÷ 1.30 for first 3 contracts
-- ============================================================
UPDATE workspace.procurement_gold.fact_contracts
SET    revised_value = ROUND(revised_value / 1.30, 2)
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

-- B15 · Restore value_utilisation_pct to 70 for contracts SET A set to 97
-- NOTE: 70 % is a representative ACTIVE-contract baseline
-- ============================================================
UPDATE workspace.procurement_gold.fact_contracts
SET    value_utilisation_pct = 70.0
WHERE  contract_id IN (
          SELECT contract_id
          FROM   workspace.procurement_gold.fact_contracts
          WHERE  value_utilisation_pct = 97.0
          ORDER  BY contract_id ASC
          LIMIT  3
       );

SELECT contract_id, value_utilisation_pct
FROM   workspace.procurement_gold.fact_contracts
ORDER  BY contract_id ASC
LIMIT  5;

-- B16 · Subtract 5 change orders added in A16
-- ============================================================
UPDATE workspace.procurement_gold.fact_contracts
SET    change_order_count = change_order_count - 5
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

-- B17 · Restore 2 AT_RISK contracts back to ACTIVE
-- ============================================================
UPDATE workspace.procurement_gold.fact_contracts
SET    contract_status = 'ACTIVE'
WHERE  contract_id IN (
          SELECT contract_id
          FROM   workspace.procurement_gold.fact_contracts
          WHERE  contract_status = 'AT_RISK'
          ORDER  BY contract_id ASC
          LIMIT  2
       );

SELECT contract_id, contract_status
FROM   workspace.procurement_gold.fact_contracts
ORDER  BY contract_id ASC
LIMIT  5;


-- ============================================================
-- SECTION 4 : fact_project_costs  (B18 – B22)
-- ============================================================

-- B18 · Revert actual_amount: ÷ 1.40 for first 5 cost lines
-- ============================================================
UPDATE workspace.procurement_gold.fact_project_costs
SET    actual_amount = ROUND(actual_amount / 1.40, 2)
WHERE  budget_id IN (
          SELECT budget_id
          FROM   workspace.procurement_gold.fact_project_costs
          ORDER  BY budget_id ASC
          LIMIT  5
       );

SELECT budget_id, actual_amount, budget_amount
FROM   workspace.procurement_gold.fact_project_costs
ORDER  BY budget_id ASC
LIMIT  5;

-- B19 · Restore variance_amount: add back 75 000 subtracted in A19
-- ============================================================
UPDATE workspace.procurement_gold.fact_project_costs
SET    variance_amount = variance_amount + 75000
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

-- B20 · Restore 3 OVERSPENT cost lines back to ACTIVE
-- ============================================================
UPDATE workspace.procurement_gold.fact_project_costs
SET    budget_status = 'ACTIVE'
WHERE  budget_id IN (
          SELECT budget_id
          FROM   workspace.procurement_gold.fact_project_costs
          WHERE  budget_status = 'OVERSPENT'
          ORDER  BY budget_id ASC
          LIMIT  3
       );

SELECT budget_id, budget_status
FROM   workspace.procurement_gold.fact_project_costs
ORDER  BY budget_id ASC
LIMIT  5;

-- B21 · Restore utilization_pct to 85 for lines SET A set to 115
-- NOTE: 85 % is a representative on-track baseline
-- ============================================================
UPDATE workspace.procurement_gold.fact_project_costs
SET    utilization_pct = 85.0
WHERE  budget_id IN (
          SELECT budget_id
          FROM   workspace.procurement_gold.fact_project_costs
          WHERE  utilization_pct = 115.0
          ORDER  BY budget_id ASC
          LIMIT  3
       );

SELECT budget_id, utilization_pct, budget_status
FROM   workspace.procurement_gold.fact_project_costs
ORDER  BY budget_id ASC
LIMIT  5;

-- B22 · Restore remaining_amount: × 2.0 (reverses A22 × 0.50)
-- ============================================================
UPDATE workspace.procurement_gold.fact_project_costs
SET    remaining_amount = ROUND(remaining_amount * 2.0, 2)
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
-- SECTION 5 : fact_goods_receipts  (B23 – B27)
-- ============================================================

-- B23 · Restore rejection_rate to 0.05 for receipts SET A set to 0.40
-- ============================================================
UPDATE workspace.procurement_gold.fact_goods_receipts
SET    rejection_rate = 0.05
WHERE  grn_id IN (
          SELECT grn_id
          FROM   workspace.procurement_gold.fact_goods_receipts
          WHERE  rejection_rate = 0.40
          ORDER  BY grn_id ASC
          LIMIT  5
       );

SELECT grn_id, rejection_rate, acceptance_rate
FROM   workspace.procurement_gold.fact_goods_receipts
ORDER  BY grn_id ASC
LIMIT  5;

-- B24 · Remove 50 rejected units from first 5 receipts
--       GREATEST prevents negative quantities
-- ============================================================
UPDATE workspace.procurement_gold.fact_goods_receipts
SET    quantity_rejected = GREATEST(quantity_rejected - 50, 0)
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

-- B25 · Restore 3 REJECTED GRNs back to RECEIVED
-- ============================================================
UPDATE workspace.procurement_gold.fact_goods_receipts
SET    grn_status = 'RECEIVED'
WHERE  grn_id IN (
          SELECT grn_id
          FROM   workspace.procurement_gold.fact_goods_receipts
          WHERE  grn_status = 'REJECTED'
          ORDER  BY grn_id ASC
          LIMIT  3
       );

SELECT grn_id, grn_status
FROM   workspace.procurement_gold.fact_goods_receipts
ORDER  BY grn_id ASC
LIMIT  5;

-- B26 · Restore acceptance_rate to 0.95 for receipts SET A set to 0.60
-- ============================================================
UPDATE workspace.procurement_gold.fact_goods_receipts
SET    acceptance_rate = 0.95
WHERE  grn_id IN (
          SELECT grn_id
          FROM   workspace.procurement_gold.fact_goods_receipts
          WHERE  acceptance_rate = 0.60
          ORDER  BY grn_id ASC
          LIMIT  5
       );

SELECT grn_id, acceptance_rate, rejection_rate
FROM   workspace.procurement_gold.fact_goods_receipts
ORDER  BY grn_id ASC
LIMIT  5;

-- B27 · Revert receipt_value: ÷ 1.25 for first 5 GRNs
-- ============================================================
UPDATE workspace.procurement_gold.fact_goods_receipts
SET    receipt_value = ROUND(receipt_value / 1.25, 2)
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
-- SECTION 6 : fact_sales  (B28 – B31)
-- ============================================================

-- B28 · Revert revenue: ÷ 1.50 for same 5 COMPLETED sales orders
-- ============================================================
UPDATE workspace.procurement_gold.fact_sales
SET    revenue = ROUND(revenue / 1.50, 2)
WHERE  sales_order_id IN (
          SELECT sales_order_id
          FROM   workspace.procurement_gold.fact_sales
          WHERE  order_status = 'COMPLETED'
          ORDER  BY sales_order_id ASC
          LIMIT  5
       );

SELECT sales_order_id, revenue, order_status
FROM   workspace.procurement_gold.fact_sales
ORDER  BY sales_order_id ASC
LIMIT  5;

-- B29 · Restore margin_pct: add back 15 pp subtracted in A29
-- ============================================================
UPDATE workspace.procurement_gold.fact_sales
SET    margin_pct = margin_pct + 15.0
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

-- B30 · Restore 3 CANCELLED sales orders back to PENDING
-- ============================================================
UPDATE workspace.procurement_gold.fact_sales
SET    order_status = 'PENDING'
WHERE  sales_order_id IN (
          SELECT sales_order_id
          FROM   workspace.procurement_gold.fact_sales
          WHERE  order_status = 'CANCELLED'
          ORDER  BY sales_order_id ASC
          LIMIT  3
       );

SELECT sales_order_id, order_status
FROM   workspace.procurement_gold.fact_sales
ORDER  BY sales_order_id ASC
LIMIT  5;

-- B31 · Revert unit_price: ÷ 1.30 for first 5 sales orders
-- ============================================================
UPDATE workspace.procurement_gold.fact_sales
SET    unit_price = ROUND(unit_price / 1.30, 2)
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
-- SECTION 7 : fact_vendor_performance  (B32 – B34)
-- ============================================================

-- B32 · Restore overall_score to 75 for evaluations SET A set to 35
-- ============================================================
UPDATE workspace.procurement_gold.fact_vendor_performance
SET    overall_score = 75.0
WHERE  performance_id IN (
          SELECT performance_id
          FROM   workspace.procurement_gold.fact_vendor_performance
          WHERE  overall_score = 35.0
          ORDER  BY performance_id ASC
          LIMIT  3
       );

SELECT performance_id, vendor_id, overall_score, recommendation
FROM   workspace.procurement_gold.fact_vendor_performance
ORDER  BY performance_id ASC
LIMIT  5;

-- B33 · Restore recommendation to 'APPROVED' for evaluations SET A set to 'REVIEW'
-- ============================================================
UPDATE workspace.procurement_gold.fact_vendor_performance
SET    recommendation = 'APPROVED'
WHERE  performance_id IN (
          SELECT performance_id
          FROM   workspace.procurement_gold.fact_vendor_performance
          WHERE  recommendation = 'REVIEW'
          ORDER  BY performance_id ASC
          LIMIT  3
       );

SELECT performance_id, vendor_id, recommendation
FROM   workspace.procurement_gold.fact_vendor_performance
ORDER  BY performance_id ASC
LIMIT  5;

-- B34 · Restore quality_score to 70 for evaluations SET A set to 30
-- ============================================================
UPDATE workspace.procurement_gold.fact_vendor_performance
SET    quality_score = 70.0
WHERE  performance_id IN (
          SELECT performance_id
          FROM   workspace.procurement_gold.fact_vendor_performance
          WHERE  quality_score = 30.0
          ORDER  BY performance_id ASC
          LIMIT  3
       );

SELECT performance_id, vendor_id, quality_score, delivery_score
FROM   workspace.procurement_gold.fact_vendor_performance
ORDER  BY performance_id ASC
LIMIT  5;


-- ============================================================
-- SECTION 8 : dim_vendor  (B35 – B38)
-- ============================================================

-- B35 · Restore performance_rating to 4.5 for vendors SET A set to 1.5
-- ============================================================
UPDATE workspace.procurement_gold.dim_vendor
SET    performance_rating = 4.5
WHERE  vendor_id IN (
          SELECT vendor_id
          FROM   workspace.procurement_gold.dim_vendor
          WHERE  performance_rating = 1.5
          ORDER  BY vendor_id ASC
          LIMIT  3
       );

SELECT vendor_id, vendor_name, performance_rating, risk_category
FROM   workspace.procurement_gold.dim_vendor
ORDER  BY vendor_id ASC
LIMIT  5;

-- B36 · Restore risk_category to 'LOW' for vendors SET A set to 'HIGH'
-- ============================================================
UPDATE workspace.procurement_gold.dim_vendor
SET    risk_category = 'LOW'
WHERE  vendor_id IN (
          SELECT vendor_id
          FROM   workspace.procurement_gold.dim_vendor
          WHERE  risk_category = 'HIGH'
          ORDER  BY vendor_id ASC
          LIMIT  3
       );

SELECT vendor_id, vendor_name, risk_category, tier
FROM   workspace.procurement_gold.dim_vendor
ORDER  BY vendor_id ASC
LIMIT  5;

-- B37 · Restore tier to 'A' for vendors SET A downgraded to 'C'
-- ============================================================
UPDATE workspace.procurement_gold.dim_vendor
SET    tier = 'A'
WHERE  vendor_id IN (
          SELECT vendor_id
          FROM   workspace.procurement_gold.dim_vendor
          WHERE  tier = 'C'
          ORDER  BY vendor_id ASC
          LIMIT  5
       );

SELECT vendor_id, vendor_name, tier, status
FROM   workspace.procurement_gold.dim_vendor
ORDER  BY vendor_id ASC
LIMIT  5;

-- B38 · Restore SUSPENDED vendors back to ACTIVE
-- ============================================================
UPDATE workspace.procurement_gold.dim_vendor
SET    status = 'ACTIVE'
WHERE  vendor_id IN (
          SELECT vendor_id
          FROM   workspace.procurement_gold.dim_vendor
          WHERE  status = 'SUSPENDED'
          ORDER  BY vendor_id ASC
          LIMIT  2
       );

SELECT vendor_id, vendor_name, status
FROM   workspace.procurement_gold.dim_vendor
ORDER  BY vendor_id ASC
LIMIT  5;


-- ============================================================
-- SECTION 9 : dim_project  (B39 – B41)
-- ============================================================

-- B39 · Restore 3 ON_HOLD projects back to ACTIVE
-- ============================================================
UPDATE workspace.procurement_gold.dim_project
SET    project_status = 'ACTIVE'
WHERE  project_id IN (
          SELECT project_id
          FROM   workspace.procurement_gold.dim_project
          WHERE  project_status = 'ON_HOLD'
          ORDER  BY project_id ASC
          LIMIT  3
       );

SELECT project_id, project_name, project_status, risk_level
FROM   workspace.procurement_gold.dim_project
ORDER  BY project_id ASC
LIMIT  5;

-- B40 · Restore risk_level to 'LOW' for projects SET A set to 'CRITICAL'
-- ============================================================
UPDATE workspace.procurement_gold.dim_project
SET    risk_level = 'LOW'
WHERE  project_id IN (
          SELECT project_id
          FROM   workspace.procurement_gold.dim_project
          WHERE  risk_level = 'CRITICAL'
          ORDER  BY project_id ASC
          LIMIT  3
       );

SELECT project_id, project_name, risk_level
FROM   workspace.procurement_gold.dim_project
ORDER  BY project_id ASC
LIMIT  5;

-- B41 · Revert approved_budget_usd: ÷ 1.35 for first 3 projects
-- ============================================================
UPDATE workspace.procurement_gold.dim_project
SET    approved_budget_usd = ROUND(approved_budget_usd / 1.35, 2)
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
-- SECTION 10 : dim_material  (B42 – B44)
-- ============================================================

-- B42 · Revert unit_price: ÷ 1.45 for same 5 active materials
-- ============================================================
UPDATE workspace.procurement_gold.dim_material
SET    unit_price = ROUND(unit_price / 1.45, 2)
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

-- B43 · Restore lead_time_days to 10 for materials SET A set to 60
-- ============================================================
UPDATE workspace.procurement_gold.dim_material
SET    lead_time_days = 10
WHERE  material_id IN (
          SELECT material_id
          FROM   workspace.procurement_gold.dim_material
          WHERE  lead_time_days = 60
          ORDER  BY material_id ASC
          LIMIT  5
       );

SELECT material_id, material_name, lead_time_days
FROM   workspace.procurement_gold.dim_material
ORDER  BY material_id ASC
LIMIT  5;

-- B44 · Reactivate 3 materials SET A deactivated
-- ============================================================
UPDATE workspace.procurement_gold.dim_material
SET    is_active = TRUE
WHERE  material_id IN (
          SELECT material_id
          FROM   workspace.procurement_gold.dim_material
          WHERE  is_active = FALSE
          ORDER  BY material_id ASC
          LIMIT  3
       );

SELECT material_id, material_name, is_active
FROM   workspace.procurement_gold.dim_material
ORDER  BY material_id ASC
LIMIT  5;

-- ============================================================
-- END OF SET B — All workspace.procurement_gold values restored.
-- Refresh Power BI Desktop to confirm charts return to baseline.
-- ============================================================
