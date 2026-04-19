-- ═══════════════════════════════════════════════════════════════
-- Cube 11: Delivery Performance
-- Schema: workspace.procurement_semantic
-- Source: gold fact_purchase_orders, fact_goods_receipts, dim_vendor, dim_date
-- ═══════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW workspace.procurement_semantic.cube_delivery_performance
COMMENT 'On-time delivery trends by vendor and period'
AS
WITH po_delivery AS (
    SELECT
        fp.po_id,
        fp.vendor_id,
        fp.po_date,
        fp.expected_delivery_date,
        fg.receipt_date                     AS actual_delivery_date,
        DATEDIFF(fg.receipt_date, fp.expected_delivery_date) AS delivery_variance_days,
        CASE
            WHEN fg.receipt_date <= fp.expected_delivery_date THEN 'ON_TIME'
            WHEN DATEDIFF(fg.receipt_date, fp.expected_delivery_date) <= 7 THEN 'LATE_1_WEEK'
            WHEN DATEDIFF(fg.receipt_date, fp.expected_delivery_date) <= 30 THEN 'LATE_1_MONTH'
            ELSE 'LATE_OVER_MONTH'
        END                                 AS delivery_class,
        fp.total_amount
    FROM workspace.procurement_gold.fact_purchase_orders  fp
    JOIN workspace.procurement_gold.fact_goods_receipts   fg ON fp.po_id = fg.po_id
)
SELECT
    pd.vendor_id,
    dv.vendor_name,
    dv.vendor_sector,
    dd.year,
    dd.quarter,
    dd.year_month,
    COUNT(*)                                AS delivery_count,
    AVG(pd.delivery_variance_days)          AS avg_variance_days,
    MAX(pd.delivery_variance_days)          AS max_variance_days,
    MIN(pd.delivery_variance_days)          AS min_variance_days,
    -- On-time rate
    ROUND(
        COUNT(CASE WHEN pd.delivery_class = 'ON_TIME' THEN 1 END)
        / NULLIF(COUNT(*), 0) * 100, 2
    )                                       AS on_time_rate_pct,
    COUNT(CASE WHEN pd.delivery_class = 'ON_TIME' THEN 1 END)      AS on_time_count,
    COUNT(CASE WHEN pd.delivery_class = 'LATE_1_WEEK' THEN 1 END)  AS late_1wk_count,
    COUNT(CASE WHEN pd.delivery_class = 'LATE_1_MONTH' THEN 1 END) AS late_1mo_count,
    COUNT(CASE WHEN pd.delivery_class = 'LATE_OVER_MONTH' THEN 1 END) AS late_over_mo_count,
    SUM(pd.total_amount)                    AS total_value_delivered,
    -- Trend: on-time rate vs previous quarter
    LAG(
        ROUND(
            COUNT(CASE WHEN pd.delivery_class = 'ON_TIME' THEN 1 END)
            / NULLIF(COUNT(*), 0) * 100, 2
        )
    ) OVER (
        PARTITION BY pd.vendor_id
        ORDER BY dd.year, dd.quarter
    )                                       AS prev_quarter_on_time_pct
FROM po_delivery                            pd
JOIN workspace.procurement_gold.dim_vendor        dv ON pd.vendor_id = dv.vendor_id AND dv._is_current = TRUE
JOIN workspace.procurement_gold.dim_date          dd ON pd.po_date = dd.date
GROUP BY
    pd.vendor_id, dv.vendor_name, dv.vendor_sector,
    dd.year, dd.quarter, dd.year_month;
