-- ═══════════════════════════════════════════════════════════════
-- Cube 01: Procurement Spend Analysis
-- Schema: procurement_dev.semantic
-- Source: gold fact_purchase_orders, dim_vendor, dim_project, dim_date
-- ═══════════════════════════════════════════════════════════════

CREATE OR REPLACE MATERIALIZED VIEW procurement_dev.semantic.cube_procurement_spend
COMMENT 'Total procurement spend by vendor, project, period with YoY comparison'
AS
SELECT
    dv.vendor_id,
    dv.vendor_name,
    dv.sector          AS vendor_sector,
    dp.project_id,
    dp.project_name,
    dd.year,
    dd.quarter,
    dd.year_month,
    -- Spend measures
    COUNT(DISTINCT fpo.po_id)           AS po_count,
    SUM(fpo.total_amount)               AS total_spend,
    SUM(fpo.line_total)                 AS total_line_value,
    SUM(fpo.total_quantity)             AS total_quantity,
    AVG(fpo.total_amount)               AS avg_po_value,
    -- YoY comparison via LAG
    LAG(SUM(fpo.total_amount), 1) OVER (
        PARTITION BY dv.vendor_id, dd.quarter
        ORDER BY dd.year
    )                                   AS prev_year_spend,
    CASE
        WHEN LAG(SUM(fpo.total_amount), 1) OVER (
            PARTITION BY dv.vendor_id, dd.quarter ORDER BY dd.year) > 0
        THEN ROUND(
            (SUM(fpo.total_amount) - LAG(SUM(fpo.total_amount), 1) OVER (
                PARTITION BY dv.vendor_id, dd.quarter ORDER BY dd.year))
            / LAG(SUM(fpo.total_amount), 1) OVER (
                PARTITION BY dv.vendor_id, dd.quarter ORDER BY dd.year) * 100, 2)
        ELSE NULL
    END                                 AS yoy_growth_pct
FROM procurement_dev.gold.fact_purchase_orders  fpo
JOIN procurement_dev.gold.dim_vendor            dv  ON fpo.vendor_id = dv.vendor_id AND dv._is_current = TRUE
JOIN procurement_dev.gold.dim_project           dp  ON fpo.project_id = dp.project_id AND dp._is_current = TRUE
JOIN procurement_dev.gold.dim_date              dd  ON fpo.po_date = dd.full_date
GROUP BY
    dv.vendor_id, dv.vendor_name, dv.sector,
    dp.project_id, dp.project_name,
    dd.year, dd.quarter, dd.year_month;
