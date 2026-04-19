-- ═══════════════════════════════════════════════════════════════
-- Cube 06: Goods Receipt Quality
-- Schema: workspace.procurement_semantic
-- Source: gold fact_goods_receipts, dim_vendor, dim_material
-- ═══════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW workspace.procurement_semantic.cube_goods_receipt_quality
COMMENT 'Goods receipt acceptance rates and defect patterns by vendor and material'
AS
SELECT
    dv.vendor_id,
    dv.vendor_name,
    dv.vendor_sector,
    dm.material_id,
    dm.material_name,
    dm.category                             AS material_category,
    COUNT(*)                                AS receipt_count,
    SUM(fg.quantity_received)               AS total_qty_received,
    SUM(fg.quantity_accepted)               AS total_qty_accepted,
    SUM(fg.quantity_rejected)               AS total_qty_rejected,
    ROUND(AVG(fg.acceptance_rate), 4)       AS avg_acceptance_rate,
    MIN(fg.acceptance_rate)                 AS min_acceptance_rate,
    SUM(fg.receipt_value)                   AS total_receipt_value,
    -- Defect rate
    ROUND(
        SUM(fg.quantity_rejected)
        / NULLIF(SUM(fg.quantity_received), 0) * 100, 2
    )                                       AS defect_rate_pct,
    -- Quality tier
    CASE
        WHEN AVG(fg.acceptance_rate) >= 0.98 THEN 'EXCELLENT'
        WHEN AVG(fg.acceptance_rate) >= 0.95 THEN 'GOOD'
        WHEN AVG(fg.acceptance_rate) >= 0.90 THEN 'ACCEPTABLE'
        ELSE 'POOR'
    END                                     AS quality_tier,
    -- Rejection value estimate
    ROUND(
        SUM(fg.receipt_value) * SUM(fg.quantity_rejected)
        / NULLIF(SUM(fg.quantity_received), 0), 2
    )                                       AS estimated_rejection_value
FROM workspace.procurement_gold.fact_goods_receipts fg
JOIN workspace.procurement_gold.dim_vendor         dv ON fg.vendor_id = dv.vendor_id AND dv._is_current = TRUE
JOIN workspace.procurement_gold.dim_material       dm ON fg.material_id = dm.material_id AND dm._is_current = TRUE
GROUP BY
    dv.vendor_id, dv.vendor_name, dv.vendor_sector,
    dm.material_id, dm.material_name, dm.category;
