-- ═══════════════════════════════════════════════════════════════
-- Cube 04: Invoice Aging
-- Schema: workspace.procurement_semantic
-- Source: gold fact_invoices, dim_vendor, dim_project, dim_date
-- ═══════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW workspace.procurement_semantic.cube_invoice_aging
COMMENT 'Invoice aging analysis by bucket, vendor, and period'
AS
SELECT
    fi.vendor_id,
    dv.vendor_name,
    dv.vendor_sector,
    fi.project_id,
    dp.project_name,
    dd.year,
    dd.quarter,
    dd.year_month,
    fi.aging_bucket,
    COUNT(*)                                AS invoice_count,
    SUM(fi.net_amount)                      AS total_net_amount,
    SUM(fi.gross_amount)                    AS total_gross_amount,
    SUM(fi.tax_amount)                      AS total_tax_amount,
    AVG(fi.days_outstanding)                AS avg_days_outstanding,
    MAX(fi.days_outstanding)                AS max_days_outstanding,
    SUM(CASE WHEN fi.payment_status = 'OVERDUE' THEN fi.gross_amount ELSE 0 END)
                                            AS overdue_amount,
    COUNT(CASE WHEN fi.payment_status = 'OVERDUE' THEN 1 END)
                                            AS overdue_count,
    ROUND(
        SUM(CASE WHEN fi.payment_status = 'OVERDUE' THEN fi.gross_amount ELSE 0 END)
        / NULLIF(SUM(fi.gross_amount), 0) * 100, 2
    )                                       AS overdue_pct
FROM workspace.procurement_gold.fact_invoices     fi
JOIN workspace.procurement_gold.dim_vendor        dv ON fi.vendor_id = dv.vendor_id AND dv._is_current = TRUE
JOIN workspace.procurement_gold.dim_project       dp ON fi.project_id = dp.project_id AND dp._is_current = TRUE
JOIN workspace.procurement_gold.dim_date          dd ON fi.invoice_date = dd.date
GROUP BY
    fi.vendor_id, dv.vendor_name, dv.vendor_sector,
    fi.project_id, dp.project_name,
    dd.year, dd.quarter, dd.year_month,
    fi.aging_bucket;
