-- ═══════════════════════════════════════════════════════════════
-- Cube 15: Procurement Efficiency
-- Schema: workspace.procurement_semantic
-- Source: gold fact_purchase_orders, fact_goods_receipts, fact_invoices, dim_vendor, dim_date
-- ═══════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW workspace.procurement_semantic.cube_procurement_efficiency
COMMENT 'Process cycle time analysis across procure-to-pay lifecycle'
AS
WITH p2p_cycle AS (
    SELECT
        fp.po_id,
        fp.vendor_id,
        fp.po_date,
        fg.receipt_date,
        fi.invoice_date,
        fi.payment_date,
        -- Cycle times
        DATEDIFF(fg.receipt_date, fp.po_date)           AS order_to_receipt_days,
        DATEDIFF(fi.invoice_date, fg.receipt_date)      AS receipt_to_invoice_days,
        DATEDIFF(fi.payment_date, fi.invoice_date)      AS invoice_to_payment_days,
        DATEDIFF(fi.payment_date, fp.po_date)           AS total_p2p_days,
        fp.total_amount
    FROM workspace.procurement_gold.fact_purchase_orders  fp
    LEFT JOIN workspace.procurement_gold.fact_goods_receipts  fg ON fp.po_id = fg.po_id
    LEFT JOIN workspace.procurement_gold.fact_invoices         fi ON fp.po_id = fi.po_id
)
SELECT
    p.vendor_id,
    dv.vendor_name,
    dv.vendor_sector,
    dd.year,
    dd.quarter,
    dd.year_month,
    COUNT(*)                                        AS po_count,
    -- Order to Receipt
    AVG(p.order_to_receipt_days)                    AS avg_order_to_receipt_days,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY p.order_to_receipt_days) AS median_order_to_receipt,
    -- Receipt to Invoice
    AVG(p.receipt_to_invoice_days)                  AS avg_receipt_to_invoice_days,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY p.receipt_to_invoice_days) AS median_receipt_to_invoice,
    -- Invoice to Payment
    AVG(p.invoice_to_payment_days)                  AS avg_invoice_to_payment_days,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY p.invoice_to_payment_days) AS median_invoice_to_payment,
    -- Total P2P cycle
    AVG(p.total_p2p_days)                           AS avg_total_p2p_days,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY p.total_p2p_days) AS median_total_p2p,
    MIN(p.total_p2p_days)                           AS min_p2p_days,
    MAX(p.total_p2p_days)                           AS max_p2p_days,
    SUM(p.total_amount)                             AS total_spend,
    -- Efficiency trend vs previous quarter
    LAG(AVG(p.total_p2p_days)) OVER (
        PARTITION BY p.vendor_id
        ORDER BY dd.year, dd.quarter
    )                                               AS prev_quarter_avg_p2p_days,
    -- % with complete P2P data
    ROUND(
        COUNT(CASE WHEN p.total_p2p_days IS NOT NULL THEN 1 END)
        / NULLIF(COUNT(*), 0) * 100, 2
    )                                               AS complete_p2p_pct
FROM p2p_cycle                              p
JOIN workspace.procurement_gold.dim_vendor        dv ON p.vendor_id = dv.vendor_id AND dv._is_current = TRUE
JOIN workspace.procurement_gold.dim_date          dd ON p.po_date = dd.date
GROUP BY
    p.vendor_id, dv.vendor_name, dv.vendor_sector,
    dd.year, dd.quarter, dd.year_month;
