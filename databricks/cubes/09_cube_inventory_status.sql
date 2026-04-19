-- ═══════════════════════════════════════════════════════════════
-- Cube 09: Inventory Movement Analysis
-- Schema: workspace.procurement_semantic
-- Source: gold fact_inventory, dim_material, dim_date
-- ═══════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW workspace.procurement_semantic.cube_inventory_status
COMMENT 'Inventory movement analysis — receipts, issues, returns, and net flow by material and period'
AS
WITH movement_summary AS (
    SELECT
        fi.material_id,
        fi.movement_type,
        dd.year,
        dd.quarter,
        dd.year_month,
        COUNT(*)                                        AS movement_count,
        SUM(fi.quantity)                                AS total_quantity,
        SUM(fi.total_value)                             AS total_value,
        AVG(fi.unit_cost)                               AS avg_unit_cost
    FROM workspace.procurement_gold.fact_inventory     fi
    JOIN workspace.procurement_gold.dim_date           dd ON fi.movement_date = dd.date
    GROUP BY fi.material_id, fi.movement_type, dd.year, dd.quarter, dd.year_month
)
SELECT
    dm.material_id,
    dm.material_name,
    dm.category                                         AS material_category,
    ms.year,
    ms.quarter,
    ms.year_month,
    -- Movement counts by type
    SUM(CASE WHEN ms.movement_type = 'RECEIPT' THEN ms.movement_count ELSE 0 END)  AS receipt_count,
    SUM(CASE WHEN ms.movement_type = 'ISSUE'   THEN ms.movement_count ELSE 0 END)  AS issue_count,
    SUM(CASE WHEN ms.movement_type = 'RETURN'  THEN ms.movement_count ELSE 0 END)  AS return_count,
    SUM(ms.movement_count)                              AS total_movement_count,
    -- Quantities by type
    SUM(CASE WHEN ms.movement_type = 'RECEIPT' THEN ms.total_quantity ELSE 0 END)   AS qty_received,
    SUM(CASE WHEN ms.movement_type = 'ISSUE'   THEN ms.total_quantity ELSE 0 END)   AS qty_issued,
    SUM(CASE WHEN ms.movement_type = 'RETURN'  THEN ms.total_quantity ELSE 0 END)   AS qty_returned,
    -- Net flow: receipts + returns - issues
    SUM(CASE WHEN ms.movement_type IN ('RECEIPT','RETURN') THEN ms.total_quantity ELSE 0 END)
    - SUM(CASE WHEN ms.movement_type = 'ISSUE' THEN ms.total_quantity ELSE 0 END)  AS net_quantity_flow,
    -- Values by type
    SUM(CASE WHEN ms.movement_type = 'RECEIPT' THEN ms.total_value ELSE 0 END)     AS receipt_value,
    SUM(CASE WHEN ms.movement_type = 'ISSUE'   THEN ms.total_value ELSE 0 END)     AS issue_value,
    SUM(CASE WHEN ms.movement_type = 'RETURN'  THEN ms.total_value ELSE 0 END)     AS return_value,
    SUM(ms.total_value)                                 AS total_movement_value,
    -- Average unit cost across all movements
    AVG(ms.avg_unit_cost)                               AS avg_unit_cost,
    -- Turnover indicator: issue qty / receipt qty
    ROUND(
        SUM(CASE WHEN ms.movement_type = 'ISSUE' THEN ms.total_quantity ELSE 0 END)
        / NULLIF(SUM(CASE WHEN ms.movement_type = 'RECEIPT' THEN ms.total_quantity ELSE 0 END), 0)
    , 2)                                                AS issue_to_receipt_ratio
FROM movement_summary                       ms
JOIN workspace.procurement_gold.dim_material      dm ON ms.material_id = dm.material_id AND dm._is_current = TRUE
GROUP BY
    dm.material_id, dm.material_name, dm.category,
    ms.year, ms.quarter, ms.year_month;
