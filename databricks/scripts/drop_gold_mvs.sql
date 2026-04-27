-- Drop existing Materialized Views before re-deploying as Streaming Tables
-- Run this in Databricks SQL BEFORE triggering a full pipeline refresh
DROP MATERIALIZED VIEW IF EXISTS workspace.procurement_gold.dim_vendor;
DROP MATERIALIZED VIEW IF EXISTS workspace.procurement_gold.dim_project;
DROP MATERIALIZED VIEW IF EXISTS workspace.procurement_gold.dim_material;
DROP MATERIALIZED VIEW IF EXISTS workspace.procurement_gold.fact_purchase_orders;
DROP MATERIALIZED VIEW IF EXISTS workspace.procurement_gold.fact_invoices;
DROP MATERIALIZED VIEW IF EXISTS workspace.procurement_gold.fact_goods_receipts;
DROP MATERIALIZED VIEW IF EXISTS workspace.procurement_gold.fact_project_costs;
DROP MATERIALIZED VIEW IF EXISTS workspace.procurement_gold.fact_contracts;
DROP MATERIALIZED VIEW IF EXISTS workspace.procurement_gold.fact_vendor_performance;
DROP MATERIALIZED VIEW IF EXISTS workspace.procurement_gold.fact_sales;
