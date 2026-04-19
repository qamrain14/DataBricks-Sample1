-- ═══════════════════════════════════════════════════════════════
-- Cube 07: Budget vs Actual
-- Schema: workspace.procurement_semantic
-- Source: gold fact_project_costs, dim_project
-- ═══════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW workspace.procurement_semantic.cube_budget_vs_actual
COMMENT 'Detailed budget vs actual variance analysis by WBS and cost type'
AS
SELECT
    dp.project_id,
    dp.project_name,
    dp.sector,
    dp.region,
    fc.wbs_element,
    fc.cost_type,
    SUM(fc.budget_amount)                   AS budget_amount,
    SUM(fc.actual_amount)                   AS actual_amount,
    SUM(fc.committed_amount)                AS committed_amount,
    SUM(fc.actual_amount + fc.committed_amount) AS total_exposure,
    SUM(fc.variance_amount)                 AS variance_amount,
    AVG(fc.variance_pct)                    AS avg_variance_pct,
    -- Variance classification
    CASE
        WHEN SUM(fc.variance_amount) > 0 THEN 'UNDER_BUDGET'
        WHEN SUM(fc.variance_amount) < -SUM(fc.budget_amount) * 0.10 THEN 'CRITICAL_OVERRUN'
        WHEN SUM(fc.variance_amount) < 0 THEN 'OVER_BUDGET'
        ELSE 'ON_BUDGET'
    END                                     AS variance_class,
    -- CPI at WBS/cost-type level
    ROUND(
        SUM(fc.budget_amount)
        / NULLIF(SUM(fc.actual_amount + fc.committed_amount), 0), 4
    )                                       AS cpi,
    -- % of project budget consumed
    ROUND(
        SUM(fc.actual_amount)
        / NULLIF(SUM(fc.budget_amount), 0) * 100, 2
    )                                       AS budget_consumed_pct
FROM workspace.procurement_gold.fact_project_costs fc
JOIN workspace.procurement_gold.dim_project       dp  ON fc.project_id = dp.project_id AND dp._is_current = TRUE
GROUP BY
    dp.project_id, dp.project_name, dp.sector, dp.region,
    fc.wbs_element, fc.cost_type;
