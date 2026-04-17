-- ═══════════════════════════════════════════════════════════════
-- Cube 03: Contract Status
-- Schema: procurement_dev.semantic
-- Source: gold fact_contracts, dim_contract, dim_vendor, dim_project
-- ═══════════════════════════════════════════════════════════════

CREATE OR REPLACE MATERIALIZED VIEW procurement_dev.semantic.cube_contract_status
COMMENT 'Contract portfolio overview with risk flags and utilisation'
AS
SELECT
    dc.contract_id,
    dc.contract_type,
    dc.status,
    dv.vendor_id,
    dv.vendor_name,
    dp.project_id,
    dp.project_name,
    dc.start_date,
    dc.end_date,
    dc.original_value,
    dc.revised_value,
    fc.items_total,
    fc.item_count,
    dc.contract_duration_days,
    dc.cost_growth_pct,
    fc.value_utilisation_pct,
    -- Risk flags
    CASE
        WHEN dc.cost_growth_pct > 20 THEN 'HIGH_COST_RISK'
        WHEN dc.cost_growth_pct > 10 THEN 'MEDIUM_COST_RISK'
        ELSE 'LOW_COST_RISK'
    END                                     AS cost_risk_flag,
    CASE
        WHEN dc.status = 'ACTIVE'
             AND dc.end_date < CURRENT_DATE() THEN 'OVERDUE'
        WHEN dc.status = 'ACTIVE'
             AND DATEDIFF(dc.end_date, CURRENT_DATE()) < 30 THEN 'NEAR_EXPIRY'
        ELSE 'ON_TRACK'
    END                                     AS schedule_risk_flag,
    -- Remaining value
    ROUND(dc.revised_value - COALESCE(fc.items_total, 0), 2) AS remaining_value
FROM procurement_dev.gold.fact_contracts    fc
JOIN procurement_dev.gold.dim_contract      dc ON fc.contract_id = dc.contract_id AND dc._is_current = TRUE
JOIN procurement_dev.gold.dim_vendor        dv ON fc.vendor_id = dv.vendor_id AND dv._is_current = TRUE
JOIN procurement_dev.gold.dim_project       dp ON fc.project_id = dp.project_id AND dp._is_current = TRUE;
