# Procurement Analytics — Power BI Report Specification

## Data Source
- **Platform**: Databricks Unity Catalog via Databricks Power BI connector
- **Host**: `dbc-760a206e-c226.cloud.databricks.com`
- **SQL Warehouse**: `/sql/1.0/warehouses/7655fa24e271f9d1`
- **Catalog**: `workspace`
- **Schemas**: `procurement_gold` (dims + facts), `procurement_semantic` (cubes)
- **Refresh**: Import mode, manual refresh (free tier)

## Model
- **BIM file**: `powerbi/model/procurement_model.bim` (Tabular Editor 2)
- **Tables**: 9 dimensions, 9 facts, 15 cubes (33 total)
- **Relationships**: 33 active relationships (star schema)
- **DAX Measures**: 25 pre-built measures

## Report Pages

### 1. Executive Summary
- KPI cards: Total Spend, Total Revenue, Active Contracts, Vendor Count, Project Count
- Spend trend (line chart by year_month from cube_procurement_spend)
- Budget vs Actual donut chart (from cube_budget_vs_actual)
- Top 10 vendors by spend (bar chart)

### 2. Procurement Spend
- Source: `cube_procurement_spend`, `cube_spend_by_sector`
- Spend by vendor sector (treemap)
- Spend by project (stacked bar)
- YoY growth table
- Slicer: Year, Quarter, Vendor Sector

### 3. Vendor Performance
- Source: `cube_vendor_performance`, `cube_vendor_risk`
- Vendor scorecard matrix (delivery, quality, commercial, HSE)
- Risk tier distribution (pie chart)
- Composite risk score scatter plot
- Slicer: Vendor Sector, Tier, Country

### 4. Invoice & Payment
- Source: `cube_invoice_aging`
- Aging buckets stacked column chart
- Overdue amount by vendor (bar)
- Avg days outstanding trend (line)
- Slicer: Year, Quarter, Aging Bucket

### 5. Project Costs & Budget
- Source: `cube_project_costs`, `cube_budget_vs_actual`, `cube_cost_variance`
- Budget utilization gauge
- CPI by project (bar chart, conditional formatting)
- Overrun severity heatmap
- Slicer: Project, Sector, Cost Type

### 6. Delivery & Quality
- Source: `cube_delivery_performance`, `cube_goods_receipt_quality`
- On-time delivery rate trend (line)
- Quality tier distribution (pie)
- Defect rate by material category (bar)
- Slicer: Vendor, Material Category, Year

### 7. Contract Management
- Source: `cube_contract_status`
- Contract value by status (donut)
- Cost growth % by contract (bar, sorted desc)
- Risk flags table (conditional icons)
- Slicer: Contract Type, Status, Vendor

### 8. Sales Analysis
- Source: `cube_sales_analysis`
- Revenue by product (bar)
- Margin % by customer type (column)
- Revenue YoY growth trend (line)
- Slicer: Year, Quarter, Customer Type, Order Type

### 9. Inventory
- Source: `cube_inventory_status`
- Net quantity flow by material (waterfall)
- Issue-to-receipt ratio trend (line)
- Movement value by category (stacked area)
- Slicer: Material Category, Year, Quarter

### 10. P2P Efficiency
- Source: `cube_procurement_efficiency`
- Avg P2P cycle days trend (line)
- Median cycle breakdown (stacked bar: order→receipt, receipt→invoice, invoice→payment)
- Top/bottom vendors by P2P days (table)
- Slicer: Vendor, Year, Quarter

### 11. Project Timeline
- Source: `cube_project_timeline`
- Schedule status distribution (donut)
- Schedule variance by project (bar)
- Cost % complete vs planned % complete scatter
- Slicer: Project Status, Sector, Priority

## Color Theme
- Primary: `#1B3A5C` (dark navy)
- Accent: `#E8792B` (orange)
- Positive: `#2E8B57`
- Negative: `#DC3545`
- Background: `#F8F9FA`
