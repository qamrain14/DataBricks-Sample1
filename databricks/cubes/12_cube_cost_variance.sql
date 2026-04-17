-- ═══════════════════════════════════════════════════════════════
-- Cube 12: Cost Variance
-- Schema: procurement_dev.semantic
-- Source: gold fact_project_costs, dim_project
-- ═══════════════════════════════════════════════════════════════

CREATE OR REPLACE MATERIALIZED VIEW procurement_dev.semantic.cube_cost_variance
COMMENT 'Cost overrun identification and trend analysis by project and cost type'
AS
SELECT
    dp.project_id,
    dp.project_name,
    dp.status                               AS project_status,
    dp.sector,
    dp.region,
    fc.cost_type,
    fc.wbs_element,
    SUM(fc.budget_amount)                   AS budget_amount,
    SUM(fc.actual_amount)                   AS actual_amount,
    SUM(fc.committed_amount)                AS committed_amount,
    SUM(fc.variance_amount)                 AS variance_amount,
    -- Absolute overrun
    GREATEST(SUM(fc.actual_amount + fc.committed_amount) - SUM(fc.budget_amount), 0) AS overrun_amount,
    -- Overrun severity
    CASE
        WHEN SUM(fc.actual_amount + fc.committed_amount) <= SUM(fc.budget_amount) THEN 'WITHIN_BUDGET'
        WHEN (SUM(fc.actual_amount + fc.committed_amount) - SUM(fc.budget_amount))
             / NULLIF(SUM(fc.budget_amount), 0) <= 0.05 THEN 'MINOR_OVERRUN'
        WHEN (SUM(fc.actual_amount + fc.committed_amount) - SUM(fc.budget_amount))
             / NULLIF(SUM(fc.budget_amount), 0) <= 0.15 THEN 'MODERATE_OVERRUN'
        ELSE 'SEVERE_OVERRUN'
    END                                     AS overrun_severity,
    -- CPI
    ROUND(
        SUM(fc.budget_amount)
        / NULLIF(SUM(fc.actual_amount + fc.committed_amount), 0), 4
    )                                       AS cpi,
    -- Remaining budget
    SUM(fc.budget_amount) - SUM(fc.actual_amount) AS remaining_budget,
    -- Burn rate (actual / budget)
    ROUND(
        SUM(fc.actual_amount) / NULLIF(SUM(fc.budget_amount), 0) * 100, 2
    )                                       AS burn_rate_pct,
    -- Rank cost types by overrun within project
    DENSE_RANK() OVER (
        PARTITION BY dp.project_id
        ORDER BY SUM(fc.actual_amount + fc.committed_amount) - SUM(fc.budget_amount) DESC
    )                                       AS overrun_rank
FROM procurement_dev.gold.fact_project_costs fc
JOIN procurement_dev.gold.dim_project       dp ON fc.project_id = dp.project_id AND dp._is_current = TRUE
GROUP BY
    dp.project_id, dp.project_name, dp.status,
    dp.sector, dp.region,
    fc.cost_type, fc.wbs_element;
