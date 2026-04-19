-- ═══════════════════════════════════════════════════════════════
-- Cube 05: Project Costs
-- Schema: workspace.procurement_semantic
-- Source: gold fact_project_costs, dim_project
-- ═══════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW workspace.procurement_semantic.cube_project_costs
COMMENT 'Project cost performance with CPI and variance analysis'
AS
SELECT
    dp.project_id,
    dp.project_name,
    dp.status                               AS project_status,
    dp.sector,
    dp.region,
    dp.priority,
    dp.project_duration_days,
    SUM(fc.budget_amount)                   AS total_budget,
    SUM(fc.actual_amount)                   AS total_actual,
    SUM(fc.committed_amount)                AS total_committed,
    SUM(fc.actual_amount + fc.committed_amount) AS total_exposure,
    SUM(fc.variance_amount)                 AS total_variance,
    AVG(fc.variance_pct)                    AS avg_variance_pct,
    -- CPI: budget / (actual + committed); > 1 = under budget
    ROUND(
        SUM(fc.budget_amount)
        / NULLIF(SUM(fc.actual_amount + fc.committed_amount), 0), 4
    )                                       AS cpi,
    -- Over-budget flag
    CASE
        WHEN SUM(fc.actual_amount + fc.committed_amount) > SUM(fc.budget_amount)
        THEN TRUE ELSE FALSE
    END                                     AS is_over_budget,
    -- Budget utilisation
    ROUND(
        SUM(fc.actual_amount) / NULLIF(SUM(fc.budget_amount), 0) * 100, 2
    )                                       AS budget_utilisation_pct,
    COUNT(DISTINCT fc.wbs_element)          AS wbs_count,
    COUNT(DISTINCT fc.cost_type)            AS cost_type_count
FROM workspace.procurement_gold.fact_project_costs fc
JOIN workspace.procurement_gold.dim_project        dp ON fc.project_id = dp.project_id AND dp._is_current = TRUE
GROUP BY
    dp.project_id, dp.project_name, dp.status,
    dp.sector, dp.region, dp.priority,
    dp.project_duration_days;
