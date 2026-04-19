-- ═══════════════════════════════════════════════════════════════
-- Cube 13: Vendor Risk
-- Schema: workspace.procurement_semantic
-- Source: gold dim_vendor, gold fact_vendor_performance, gold fact_invoices, gold fact_goods_receipts
-- ═══════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW workspace.procurement_semantic.cube_vendor_risk
COMMENT 'Composite vendor risk scoring combining performance, financial, and quality signals'
AS
WITH perf_agg AS (
    SELECT
        vendor_id,
        AVG(overall_score)                  AS avg_overall_score,
        AVG(delivery_score)                 AS avg_delivery_score,
        AVG(quality_score)                  AS avg_quality_score,
        AVG(commercial_score)               AS avg_commercial_score,
        AVG(hse_score)                      AS avg_hse_score,
        MIN(overall_score)                  AS min_overall_score,
        COUNT(*)                            AS evaluation_count
    FROM workspace.procurement_gold.fact_vendor_performance
    GROUP BY vendor_id
),
invoice_agg AS (
    SELECT
        vendor_id,
        COUNT(*)                            AS invoice_count,
        SUM(CASE WHEN payment_status = 'OVERDUE' THEN 1 ELSE 0 END) AS overdue_invoices,
        AVG(days_outstanding)               AS avg_days_outstanding,
        SUM(gross_amount)                   AS total_invoiced
    FROM workspace.procurement_gold.fact_invoices
    GROUP BY vendor_id
),
quality_agg AS (
    SELECT
        vendor_id,
        AVG(acceptance_rate)                AS avg_acceptance_rate,
        SUM(quantity_rejected)              AS total_rejected
    FROM workspace.procurement_gold.fact_goods_receipts
    GROUP BY vendor_id
)
SELECT
    dv.vendor_id,
    dv.vendor_name,
    dv.vendor_sector,
    dv.country,
    dv.city,
    dv.tier,
    dv.status                               AS vendor_status,
    -- Performance scores
    pa.avg_overall_score,
    pa.avg_delivery_score,
    pa.avg_quality_score,
    pa.avg_commercial_score,
    pa.avg_hse_score,
    pa.min_overall_score,
    pa.evaluation_count,
    -- Financial signals
    ia.invoice_count,
    ia.overdue_invoices,
    ia.avg_days_outstanding,
    ia.total_invoiced,
    ROUND(ia.overdue_invoices / NULLIF(ia.invoice_count, 0) * 100, 2) AS overdue_rate_pct,
    -- Quality signals
    qa.avg_acceptance_rate,
    qa.total_rejected,
    -- Composite risk score (0-100, lower = riskier)
    ROUND(
        COALESCE(pa.avg_overall_score, 50) * 0.35
        + COALESCE(qa.avg_acceptance_rate * 100, 50) * 0.25
        + GREATEST(0, 100 - COALESCE(ia.avg_days_outstanding, 0)) * 0.20
        + GREATEST(0, 100 - COALESCE(ia.overdue_invoices / NULLIF(ia.invoice_count, 0) * 100, 0)) * 0.20
    , 2)                                    AS composite_risk_score,
    -- Risk tier
    CASE
        WHEN COALESCE(pa.avg_overall_score, 50) * 0.35
             + COALESCE(qa.avg_acceptance_rate * 100, 50) * 0.25
             + GREATEST(0, 100 - COALESCE(ia.avg_days_outstanding, 0)) * 0.20
             + GREATEST(0, 100 - COALESCE(ia.overdue_invoices / NULLIF(ia.invoice_count, 0) * 100, 0)) * 0.20
             >= 80 THEN 'LOW_RISK'
        WHEN COALESCE(pa.avg_overall_score, 50) * 0.35
             + COALESCE(qa.avg_acceptance_rate * 100, 50) * 0.25
             + GREATEST(0, 100 - COALESCE(ia.avg_days_outstanding, 0)) * 0.20
             + GREATEST(0, 100 - COALESCE(ia.overdue_invoices / NULLIF(ia.invoice_count, 0) * 100, 0)) * 0.20
             >= 60 THEN 'MEDIUM_RISK'
        ELSE 'HIGH_RISK'
    END                                     AS risk_tier,
    -- Overall ranking
    DENSE_RANK() OVER (ORDER BY
        COALESCE(pa.avg_overall_score, 50) * 0.35
        + COALESCE(qa.avg_acceptance_rate * 100, 50) * 0.25
        + GREATEST(0, 100 - COALESCE(ia.avg_days_outstanding, 0)) * 0.20
        + GREATEST(0, 100 - COALESCE(ia.overdue_invoices / NULLIF(ia.invoice_count, 0) * 100, 0)) * 0.20
        DESC
    )                                       AS risk_rank
FROM workspace.procurement_gold.dim_vendor        dv
LEFT JOIN perf_agg                          pa ON dv.vendor_id = pa.vendor_id
LEFT JOIN invoice_agg                       ia ON dv.vendor_id = ia.vendor_id
LEFT JOIN quality_agg                       qa ON dv.vendor_id = qa.vendor_id
WHERE dv._is_current = TRUE;
