-- ═══════════════════════════════════════════════════════════════
-- Cube 08: Sales Analysis
-- Schema: workspace.procurement_semantic
-- Source: gold fact_sales, dim_sector, dim_date
-- ═══════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW workspace.procurement_semantic.cube_sales_analysis
COMMENT 'Revenue and margin analysis by product, customer, and period'
AS
SELECT
    fs.product_name,
    fs.customer_name,
    fs.customer_type,
    fs.order_type,
    dd.year,
    dd.quarter,
    dd.year_month,
    COUNT(*)                                AS sales_count,
    SUM(fs.quantity)                         AS total_quantity,
    SUM(fs.revenue)                          AS total_revenue,
    SUM(fs.cogs)                             AS total_cogs,
    SUM(fs.gross_margin)                     AS total_margin,
    ROUND(
        SUM(fs.gross_margin)
        / NULLIF(SUM(fs.revenue), 0) * 100, 2
    )                                       AS margin_pct,
    AVG(fs.unit_price)                       AS avg_unit_price,
    -- YoY revenue comparison
    LAG(SUM(fs.revenue)) OVER (
        PARTITION BY fs.product_name, dd.quarter
        ORDER BY dd.year
    )                                       AS prev_year_revenue,
    ROUND(
        (SUM(fs.revenue) - LAG(SUM(fs.revenue)) OVER (
            PARTITION BY fs.product_name, dd.quarter
            ORDER BY dd.year
        )) / NULLIF(LAG(SUM(fs.revenue)) OVER (
            PARTITION BY fs.product_name, dd.quarter
            ORDER BY dd.year
        ), 0) * 100, 2
    )                                       AS revenue_yoy_growth_pct
FROM workspace.procurement_gold.fact_sales        fs
JOIN workspace.procurement_gold.dim_date          dd ON fs.order_date = dd.date
GROUP BY
    fs.product_name, fs.customer_name, fs.customer_type,
    fs.order_type,
    dd.year, dd.quarter, dd.year_month;
