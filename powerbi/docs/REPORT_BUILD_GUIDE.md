# Power BI Report — Build Guide

Step-by-step instructions to build the Procurement Analytics report in Power BI Desktop using the generated artifacts.

---

## Prerequisites

| Item | Required | Notes |
|------|----------|-------|
| Power BI Desktop | Yes | Free from Microsoft Store |
| Databricks PAT | Yes | Settings → Developer → Access Tokens |
| Tabular Editor 2 | Optional | For importing BIM model directly |
| Simba Spark ODBC | Optional | Only if using ODBC instead of native connector |

---

## Step 0 — Import Theme

1. Open Power BI Desktop → **View** → **Themes** → **Browse for themes**
2. Select `powerbi/theme/procurement_analytics.json`
3. Theme applies navy/orange/green palette to all visuals automatically

---

## Step 1 — Connect to Databricks

### Option A: Native Databricks Connector (Recommended)

1. **Get Data** → search **Databricks** → select **Azure Databricks**
2. Enter:
   - **Server Hostname**: `dbc-760a206e-c226.cloud.databricks.com`
   - **HTTP Path**: `/sql/1.0/warehouses/7655fa24e271f9d1`
   - **Data Connectivity mode**: Import
3. **Authentication**: Personal Access Token → paste your Databricks PAT
4. In Navigator, expand **workspace** → select tables from `procurement_gold` and `procurement_semantic`
5. Select all 33 tables → **Load**

### Option B: Use Power Query M Files

1. **Get Data** → **Blank Query** → **Advanced Editor**
2. For each `.pq` file in `powerbi/connections/`:
   - Paste the M code from the file
   - Rename the query to the table name (PascalCase)
3. Repeat for all 33 tables

### Option C: Import BIM via Tabular Editor 2

1. Open Tabular Editor 2
2. **File** → **Open** → select `powerbi/model/procurement_model.bim`
3. **Model** → **Deploy** → choose your Power BI Desktop instance
4. This imports all 33 tables, 33 relationships, and 25 DAX measures at once

---

## Step 2 — Verify Data Model

After loading, switch to **Model View** and confirm:
- 9 dimension tables (Dim*) + 9 fact tables (Fact*) + 15 cubes (Cube*)
- Star schema: facts/cubes → dimensions
- 33 relationships (auto-detected or via BIM import)

If relationships are missing, create them manually per the mapping in `REPORT_SPEC.md`.

---

## Step 3 — Add DAX Measures

If you used Option A or B (not BIM), add these measures manually. Go to each table and **New Measure**:

### FactPurchaseOrders
```dax
Total Spend = SUM(FactPurchaseOrders[TotalValue])
Total PO Count = COUNTROWS(FactPurchaseOrders)
Avg PO Value = AVERAGE(FactPurchaseOrders[TotalValue])
```

### FactInvoices
```dax
Total Invoiced = SUM(FactInvoices[GrossAmount])
Total Paid = SUM(FactInvoices[PaidAmount])
Overdue Amount = CALCULATE(SUM(FactInvoices[GrossAmount]), FactInvoices[PaymentStatus] = "Overdue")
Avg Days to Pay = AVERAGE(FactInvoices[DaysToPay])
```

### FactProjectCosts
```dax
Total Budget = SUM(FactProjectCosts[BudgetAmount])
Total Actual Cost = SUM(FactProjectCosts[ActualAmount])
Budget Variance = [Total Budget] - [Total Actual Cost]
Budget Utilization % = DIVIDE([Total Actual Cost], [Total Budget], 0)
```

### FactVendorPerformance
```dax
Avg Vendor Score = AVERAGE(FactVendorPerformance[OverallScore])
```

### DimVendor
```dax
Vendor Count = COUNTROWS(DimVendor)
```

### FactGoodsReceipts
```dax
Total Received Qty = SUM(FactGoodsReceipts[QuantityReceived])
Avg Acceptance Rate = AVERAGE(FactGoodsReceipts[AcceptanceRate])
```

### FactContracts
```dax
Total Contract Value = SUM(FactContracts[ContractValue])
Active Contracts = CALCULATE(COUNTROWS(FactContracts), FactContracts[ContractStatus] = "Active")
```

### FactSales
```dax
Total Revenue = SUM(FactSales[Revenue])
Total COGS = SUM(FactSales[Cogs])
Gross Margin = SUM(FactSales[GrossMargin])
Margin % = DIVIDE([Gross Margin], [Total Revenue], 0)
```

### FactInventory
```dax
Net Inventory Flow = SUM(FactInventory[Quantity])
Inventory Value = SUM(FactInventory[TotalValue])
```

### DimProject
```dax
Project Count = COUNTROWS(DimProject)
```

### CubeProjectCosts
```dax
Over Budget Projects = CALCULATE(COUNTROWS(CubeProjectCosts), CubeProjectCosts[IsOverBudget] = TRUE())
```

---

## Step 4 — Build Report Pages

### Page 1: Executive Summary

| Visual | Type | Fields | Format |
|--------|------|--------|--------|
| Total Spend | Card | `[Total Spend]` | $ currency |
| Total Revenue | Card | `[Total Revenue]` | $ currency |
| Active Contracts | Card | `[Active Contracts]` | integer |
| Vendor Count | Card | `[Vendor Count]` | integer |
| Project Count | Card | `[Project Count]` | integer |
| Spend Trend | Line Chart | Axis: `CubeProcurementSpend[YearMonth]`, Values: `CubeProcurementSpend[TotalSpend]` | |
| Budget vs Actual | Donut Chart | Legend: `CubeBudgetVsActual[VarianceClass]`, Values: `SUM(CubeBudgetVsActual[ActualAmount])` | |
| Top 10 Vendors | Bar Chart | Axis: `CubeProcurementSpend[VendorName]`, Values: `SUM(CubeProcurementSpend[TotalSpend])`, Top N filter = 10 | Sort desc |

**Layout**: 5 KPI cards across the top row. Line chart spanning 60% width on the left, donut 40% on the right. Bar chart full width at the bottom.

---

### Page 2: Procurement Spend

| Visual | Type | Source | Fields |
|--------|------|--------|--------|
| Spend by Sector | Treemap | CubeSpendBySector | Group: `SectorName`, Values: `SUM(TotalSpend)` |
| Spend by Project | Stacked Bar | CubeProcurementSpend | Axis: `ProjectName`, Values: `SUM(TotalSpend)`, Legend: `VendorSector` |
| YoY Growth | Table | CubeProcurementSpend | Columns: `VendorName`, `SUM(TotalSpend)`, `SUM(PrevYearSpend)`, `AVG(YoyGrowthPct)` |
| **Slicers** | Slicer | CubeProcurementSpend | `Year`, `Quarter`, `VendorSector` (3 separate slicers) |

**Conditional formatting on YoY Growth**: Green if > 0, Red if < 0.

---

### Page 3: Vendor Performance

| Visual | Type | Source | Fields |
|--------|------|--------|--------|
| Scorecard Matrix | Matrix | CubeVendorPerformance | Rows: `VendorName`, Values: `AVG(AvgDeliveryScore)`, `AVG(AvgQualityScore)`, `AVG(AvgCommercialScore)`, `AVG(AvgHseScore)`, `AVG(AvgOverallScore)` |
| Risk Tier | Pie Chart | CubeVendorRisk | Legend: `RiskTier`, Values: `COUNT(VendorId)` |
| Risk Scatter | Scatter | CubeVendorRisk | X: `AvgOverallScore`, Y: `CompositeRiskScore`, Size: `TotalInvoiced`, Details: `VendorName` |
| **Slicers** | Slicer | | `Sector`, `Tier`, `VendorCountry` |

**Conditional formatting on scores**: Background color scale — red (0) → yellow (50) → green (100).

---

### Page 4: Invoice & Payment

| Visual | Type | Source | Fields |
|--------|------|--------|--------|
| Aging Buckets | Stacked Column | CubeInvoiceAging | Axis: `YearMonth`, Values: `SUM(TotalNetAmount)`, Legend: `AgingBucket` |
| Overdue by Vendor | Bar Chart | CubeInvoiceAging | Axis: `VendorName`, Values: `SUM(OverdueAmount)` | Sort desc |
| Days Outstanding Trend | Line Chart | CubeInvoiceAging | Axis: `YearMonth`, Values: `AVG(AvgDaysOutstanding)` |
| **Slicers** | Slicer | | `Year`, `Quarter`, `AgingBucket` |

---

### Page 5: Project Costs & Budget

| Visual | Type | Source | Fields |
|--------|------|--------|--------|
| Budget Utilization | Gauge | FactProjectCosts | Value: `[Budget Utilization %]`, Max: 1 |
| CPI by Project | Bar Chart | CubeProjectCosts | Axis: `ProjectName`, Values: `AVG(Cpi)` |
| Overrun Heatmap | Matrix | CubeCostVariance | Rows: `ProjectName`, Columns: `CostType`, Values: `SUM(OverrunAmount)` |
| **Slicers** | Slicer | | `ProjectName`, `Sector`, `CostType` |

**CPI bar conditional formatting**: Red if CPI < 0.9, Yellow if 0.9–1.0, Green if > 1.0.

---

### Page 6: Delivery & Quality

| Visual | Type | Source | Fields |
|--------|------|--------|--------|
| On-Time Rate Trend | Line Chart | CubeDeliveryPerformance | Axis: `YearMonth`, Values: `AVG(OnTimeRatePct)` |
| Quality Tier | Pie Chart | CubeGoodsReceiptQuality | Legend: `QualityTier`, Values: `COUNT(VendorId)` |
| Defect Rate | Bar Chart | CubeGoodsReceiptQuality | Axis: `MaterialCategory`, Values: `AVG(DefectRatePct)` |
| **Slicers** | Slicer | | `VendorName`, `MaterialCategory`, `Year` |

---

### Page 7: Contract Management

| Visual | Type | Source | Fields |
|--------|------|--------|--------|
| Value by Status | Donut | CubeContractStatus | Legend: `Status`, Values: `SUM(RevisedValue)` |
| Cost Growth | Bar Chart | CubeContractStatus | Axis: `ContractId`, Values: `AVG(CostGrowthPct)` | Sort desc |
| Risk Flags | Table | CubeContractStatus | Columns: `ContractId`, `VendorName`, `ProjectName`, `CostRiskFlag`, `ScheduleRiskFlag`, `RemainingValue` |
| **Slicers** | Slicer | | `ContractType`, `Status`, `VendorName` |

**Risk flag icons**: Use conditional formatting → Icons → checkmark (false) / warning triangle (true).

---

### Page 8: Sales Analysis

| Visual | Type | Source | Fields |
|--------|------|--------|--------|
| Revenue by Product | Bar Chart | CubeSalesAnalysis | Axis: `ProductName`, Values: `SUM(TotalRevenue)` |
| Margin by Customer Type | Column Chart | CubeSalesAnalysis | Axis: `CustomerType`, Values: `AVG(MarginPct)` |
| Revenue YoY Trend | Line Chart | CubeSalesAnalysis | Axis: `YearMonth`, Values: `SUM(TotalRevenue)`, Secondary: `AVG(RevenueYoyGrowthPct)` |
| **Slicers** | Slicer | | `Year`, `Quarter`, `CustomerType`, `OrderType` |

---

### Page 9: Inventory

| Visual | Type | Source | Fields |
|--------|------|--------|--------|
| Net Flow | Waterfall | CubeInventoryStatus | Category: `MaterialName`, Values: `SUM(NetQuantityFlow)` |
| Issue-to-Receipt Ratio | Line Chart | CubeInventoryStatus | Axis: `YearMonth`, Values: `AVG(IssueToReceiptRatio)` |
| Movement Value | Stacked Area | CubeInventoryStatus | Axis: `YearMonth`, Values: `SUM(TotalMovementValue)`, Legend: `MaterialCategory` |
| **Slicers** | Slicer | | `MaterialCategory`, `Year`, `Quarter` |

---

### Page 10: P2P Efficiency

| Visual | Type | Source | Fields |
|--------|------|--------|--------|
| Avg P2P Trend | Line Chart | CubeProcurementEfficiency | Axis: `YearMonth`, Values: `AVG(AvgTotalP2pDays)` |
| Cycle Breakdown | Stacked Bar | CubeProcurementEfficiency | Axis: `VendorName`, Values: `AVG(AvgOrderToReceiptDays)`, `AVG(AvgReceiptToInvoiceDays)`, `AVG(AvgInvoiceToPaymentDays)` |
| Top/Bottom Vendors | Table | CubeProcurementEfficiency | Columns: `VendorName`, `AVG(AvgTotalP2pDays)`, `AVG(MedianTotalP2p)`, `SUM(TotalSpend)` | Sort by P2P days |
| **Slicers** | Slicer | | `VendorName`, `Year`, `Quarter` |

---

### Page 11: Project Timeline

| Visual | Type | Source | Fields |
|--------|------|--------|--------|
| Schedule Status | Donut | CubeProjectTimeline | Legend: `ScheduleStatus`, Values: `COUNT(ProjectId)` |
| Schedule Variance | Bar Chart | CubeProjectTimeline | Axis: `ProjectName`, Values: `SUM(ScheduleVarianceDays)` |
| Cost vs Plan Scatter | Scatter | CubeProjectTimeline | X: `PlannedPctComplete`, Y: `CostPctComplete`, Size: `TotalBudget`, Details: `ProjectName` |
| **Slicers** | Slicer | | `ProjectStatus`, `Sector`, `Priority` |

---

## Step 5 — Final Polish

1. **Add page navigation**: Insert → Buttons → Page navigator (auto-generates tab bar)
2. **Add report title**: Text box on each page with page name in Segoe UI Semibold 16pt, color `#1B3A5C`
3. **Add logo/branding**: Insert → Image → position top-left corner of each page
4. **Tooltips**: Right-click visuals → add tooltip fields for drill-through context
5. **Bookmarks**: View → Bookmarks → create bookmarks for key filter states

## Step 6 — Save & Publish

1. **Save**: File → Save As → `Procurement_Analytics.pbix`
2. **Publish** (requires Power BI Pro/Premium):
   - Home → Publish → select workspace
   - Set up scheduled refresh in Power BI Service with Databricks PAT credentials

---

## Quick Reference

| Artifact | Path | Purpose |
|----------|------|---------|
| Theme | `powerbi/theme/procurement_analytics.json` | Import via View → Themes |
| BIM Model | `powerbi/model/procurement_model.bim` | Import via Tabular Editor 2 |
| PQ Files | `powerbi/connections/*.pq` | M query templates for each table |
| Report Spec | `powerbi/docs/REPORT_SPEC.md` | Visual specifications |
| ODBC Setup | `powerbi/scripts/odbc_setup.bat` | Run as admin to create DSN |
| Refresh | `powerbi/scripts/refresh.bat` | Batch refresh automation |
