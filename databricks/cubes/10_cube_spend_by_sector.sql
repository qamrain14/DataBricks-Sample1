-- ═══════════════════════════════════════════════════════════════
-- Cube 10: Spend by Sector
-- Schema: procurement_dev.semantic
-- Source: gold fact_purchase_orders, dim_vendor, dim_sector, dim_date
-- ═══════════════════════════════════════════════════════════════

CREATE OR REPLACE MATERIALIZED VIEW procurement_dev.semantic.cube_spend_by_sector
COMMENT 'Cross-sector procurement spend analysis with market share'
AS
SELECT
    ds.sector_key,
    ds.sector_name,
    dv.vendor_id,
    dv.vendor_name,
    dv.country,
    dd.year,
    dd.quarter,
    COUNT(*)                                AS po_count,
    SUM(fp.total_amount)                    AS total_spend,
    SUM(fp.total_quantity)                  AS total_quantity,
    AVG(fp.total_amount)                    AS avg_po_value,
    -- Vendor share within sector
    ROUND(
        SUM(fp.total_amount)
        / NULLIF(SUM(SUM(fp.total_amount)) OVER (
            PARTITION BY ds.sector_key, dd.year, dd.quarter
        ), 0) * 100, 2
    )                                       AS vendor_sector_share_pct,
    -- Sector rank
    DENSE_RANK() OVER (
        PARTITION BY dd.year, dd.quarter
        ORDER BY SUM(fp.total_amount) DESC
    )                                       AS sector_spend_rank,
    -- YoY sector growth
    LAG(SUM(fp.total_amount)) OVER (
        PARTITION BY ds.sector_key, dv.vendor_id, dd.quarter
        ORDER BY dd.year
    )                                       AS prev_year_spend,
    ROUND(
        (SUM(fp.total_amount) - LAG(SUM(fp.total_amount)) OVER (
            PARTITION BY ds.sector_key, dv.vendor_id, dd.quarter
            ORDER BY dd.year
        )) / NULLIF(LAG(SUM(fp.total_amount)) OVER (
            PARTITION BY ds.sector_key, dv.vendor_id, dd.quarter
            ORDER BY dd.year
        ), 0) * 100, 2
    )                                       AS yoy_growth_pct
FROM procurement_dev.gold.fact_purchase_orders fp
JOIN procurement_dev.gold.dim_vendor        dv ON fp.vendor_id = dv.vendor_id AND dv._is_current = TRUE
JOIN procurement_dev.gold.dim_sector        ds ON dv.sector = ds.sector_key
JOIN procurement_dev.gold.dim_date          dd ON fp.po_date = dd.full_date
GROUP BY
    ds.sector_key, ds.sector_name,
    dv.vendor_id, dv.vendor_name, dv.country,
    dd.year, dd.quarter;
