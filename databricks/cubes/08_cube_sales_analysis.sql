-- ═══════════════════════════════════════════════════════════════
-- Cube 08: Sales Analysis
-- Schema: workspace.procurement_semantic
-- Source: gold fact_sales, dim_sector, dim_date
-- ═══════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW workspace.procurement_semantic.cube_sales_analysis
COMMENT 'Revenue and margin analysis by product, sector, region, and period'
AS
SELECT
    fs.product_id,
    fs.product_name,
    fs.customer_id,
    ds.sector_key                           AS sector,
    ds.sector_name,
    dd.year,
    dd.quarter,
    dd.year_month,
    COUNT(*)                                AS sales_count,
    SUM(fs.quantity)                         AS total_quantity,
    SUM(fs.revenue)                          AS total_revenue,
    SUM(fs.cogs)                             AS total_cogs,
    SUM(fs.gross_margin_amount)              AS total_margin,
    ROUND(
        SUM(fs.gross_margin_amount)
        / NULLIF(SUM(fs.revenue), 0) * 100, 2
    )                                       AS margin_pct,
    AVG(fs.unit_price)                       AS avg_unit_price,
    -- YoY revenue comparison
    LAG(SUM(fs.revenue)) OVER (
        PARTITION BY fs.product_id, dd.quarter
        ORDER BY dd.year
    )                                       AS prev_year_revenue,
    ROUND(
        (SUM(fs.revenue) - LAG(SUM(fs.revenue)) OVER (
            PARTITION BY fs.product_id, dd.quarter
            ORDER BY dd.year
        )) / NULLIF(LAG(SUM(fs.revenue)) OVER (
            PARTITION BY fs.product_id, dd.quarter
            ORDER BY dd.year
        ), 0) * 100, 2
    )                                       AS revenue_yoy_growth_pct
FROM workspace.procurement_gold.fact_sales        fs
JOIN workspace.procurement_gold.dim_sector        ds ON fs.sector = ds.sector_key
JOIN workspace.procurement_gold.dim_date          dd ON fs.sale_date = dd.date
GROUP BY
    fs.product_id, fs.product_name, fs.customer_id,
    ds.sector_key, ds.sector_name,
    dd.year, dd.quarter, dd.year_month;
