-- ═══════════════════════════════════════════════════════════════
-- Cube 14: Project Timeline
-- Schema: workspace.procurement_semantic
-- Source: gold dim_project, fact_project_costs, fact_contracts
-- ═══════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW workspace.procurement_semantic.cube_project_timeline
COMMENT 'Schedule performance index and milestone tracking by project'
AS
WITH project_spend AS (
    SELECT
        project_id,
        MIN(posting_date)                   AS first_spend_date,
        MAX(posting_date)                   AS last_spend_date,
        SUM(actual_amount)                  AS total_actual,
        SUM(budget_amount)                  AS total_budget,
        COUNT(DISTINCT wbs_element)         AS active_wbs_count,
        COUNT(DISTINCT cost_type)           AS cost_type_count
    FROM workspace.procurement_gold.fact_project_costs
    GROUP BY project_id
),
project_contracts AS (
    SELECT
        project_id,
        COUNT(*)                            AS contract_count,
        SUM(CASE WHEN status = 'ACTIVE' THEN 1 ELSE 0 END) AS active_contracts,
        SUM(CASE WHEN status = 'COMPLETED' THEN 1 ELSE 0 END) AS completed_contracts,
        SUM(revised_value)                  AS total_contract_value
    FROM workspace.procurement_gold.dim_contract
    WHERE _is_current = TRUE
    GROUP BY project_id
)
SELECT
    dp.project_id,
    dp.project_name,
    dp.status                               AS project_status,
    dp.sector,
    dp.region,
    dp.priority,
    dp.start_date,
    dp.end_date,
    dp.project_duration_days                AS planned_duration_days,
    -- Actual timeline
    ps.first_spend_date,
    ps.last_spend_date,
    DATEDIFF(ps.last_spend_date, ps.first_spend_date) AS actual_active_days,
    -- Schedule variance
    CASE
        WHEN dp.status IN ('COMPLETED', 'CLOSED')
        THEN DATEDIFF(ps.last_spend_date, dp.end_date)
        ELSE DATEDIFF(CURRENT_DATE(), dp.end_date)
    END                                     AS schedule_variance_days,
    -- Schedule status
    CASE
        WHEN dp.status IN ('COMPLETED', 'CLOSED') AND ps.last_spend_date <= dp.end_date THEN 'ON_TIME'
        WHEN dp.status IN ('COMPLETED', 'CLOSED') THEN 'DELAYED'
        WHEN dp.end_date < CURRENT_DATE() THEN 'OVERDUE'
        WHEN DATEDIFF(dp.end_date, CURRENT_DATE()) < 30 THEN 'AT_RISK'
        ELSE 'ON_TRACK'
    END                                     AS schedule_status,
    -- SPI estimate: planned % complete / actual % complete
    ROUND(
        DATEDIFF(LEAST(CURRENT_DATE(), dp.end_date), dp.start_date)
        / NULLIF(dp.project_duration_days, 0), 4
    )                                       AS planned_pct_complete,
    ROUND(
        ps.total_actual / NULLIF(ps.total_budget, 0), 4
    )                                       AS cost_pct_complete,
    -- Spend & contract context
    ps.total_actual,
    ps.total_budget,
    ps.active_wbs_count,
    pc.contract_count,
    pc.active_contracts,
    pc.completed_contracts,
    pc.total_contract_value
FROM workspace.procurement_gold.dim_project       dp
LEFT JOIN project_spend                     ps ON dp.project_id = ps.project_id
LEFT JOIN project_contracts                 pc ON dp.project_id = pc.project_id
WHERE dp._is_current = TRUE;
