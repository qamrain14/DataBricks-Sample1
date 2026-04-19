-- ═══════════════════════════════════════════════════════════════
-- Cube 02: Vendor Performance
-- Schema: workspace.procurement_semantic
-- Source: gold dim_vendor, gold fact_vendor_performance
-- ═══════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW workspace.procurement_semantic.cube_vendor_performance
COMMENT 'Aggregated vendor scores with rankings and trend'
AS
SELECT
    dv.vendor_id,
    dv.vendor_name,
    dv.sector,
    dv.tier,
    dv.country,
    vp.evaluation_period,
    -- Average scores
    ROUND(AVG(vp.delivery_score), 2)      AS avg_delivery_score,
    ROUND(AVG(vp.quality_score), 2)       AS avg_quality_score,
    ROUND(AVG(vp.commercial_score), 2)    AS avg_commercial_score,
    ROUND(AVG(vp.hse_score), 2)           AS avg_hse_score,
    ROUND(AVG(vp.overall_score), 2)       AS avg_overall_score,
    -- Latest recommendation
    MAX(vp.recommendation)                AS latest_recommendation,
    COUNT(*)                              AS evaluation_count,
    -- Rank across all vendors for the period
    DENSE_RANK() OVER (
        PARTITION BY vp.evaluation_period
        ORDER BY AVG(vp.overall_score) DESC
    )                                     AS vendor_rank,
    -- Trend: diff from previous period
    ROUND(AVG(vp.overall_score) - LAG(AVG(vp.overall_score), 1) OVER (
        PARTITION BY dv.vendor_id ORDER BY vp.evaluation_period
    ), 2)                                 AS score_trend
FROM workspace.procurement_gold.dim_vendor                dv
JOIN workspace.procurement_gold.fact_vendor_performance     vp ON dv.vendor_id = vp.vendor_id AND dv._is_current = TRUE
GROUP BY
    dv.vendor_id, dv.vendor_name, dv.sector, dv.tier, dv.country,
    vp.evaluation_period;
