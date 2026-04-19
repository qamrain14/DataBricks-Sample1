# Business Process Workflow
## Procurement & Construction Data Lakehouse — Medallion Architecture

---

## 1. Executive Summary

This document describes the end-to-end business process workflow for a **Databricks Lakehouse** implementation targeting **Procurement and Construction (EPC)** operations. The solution ingests data from 15 operational domains, transforms it through a 3-layer **Medallion Architecture** (Bronze → Silver → Gold), and delivers 15 pre-built semantic cubes for real-time business intelligence.

---

## 2. Business Process Overview

### 2.1 Current-State Pain Points (AS-IS)

```
┌──────────────────────────────────────────────────────────────────────┐
│                     CURRENT STATE (MANUAL / SILOED)                  │
├──────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐            │
│  │ ERP      │  │ Project  │  │ Finance  │  │ Quality  │            │
│  │ System   │  │ Controls │  │ Module   │  │ System   │            │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘            │
│       │              │              │              │                  │
│       ▼              ▼              ▼              ▼                  │
│  ┌──────────────────────────────────────────────────────┐            │
│  │              MANUAL EXCEL CONSOLIDATION               │            │
│  │         (3–5 days per reporting cycle)                │            │
│  └──────────────────────┬───────────────────────────────┘            │
│                         │                                            │
│                         ▼                                            │
│  ┌──────────────────────────────────────────────────────┐            │
│  │      STATIC PDF/PPT REPORTS — STALE BY DELIVERY      │            │
│  │         (errors, inconsistencies, no drill-down)      │            │
│  └──────────────────────────────────────────────────────┘            │
│                                                                      │
│  ⚠ No single source of truth                                        │
│  ⚠ No real-time vendor performance visibility                       │
│  ⚠ No automated budget vs actual tracking                           │
│  ⚠ No early-warning risk detection                                  │
│  ⚠ Invoice aging unknown until cash flow crisis                     │
└──────────────────────────────────────────────────────────────────────┘
```

### 2.2 Future-State Architecture (TO-BE)

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                     FUTURE STATE — MEDALLION LAKEHOUSE                          │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  ┌─────────────────────────────────────────────────┐                            │
│  │          OPERATIONAL DATA SOURCES                │                            │
│  │  ┌─────────┐ ┌──────────┐ ┌──────────┐         │                            │
│  │  │Vendors  │ │Projects  │ │Materials │         │                            │
│  │  │(100K)   │ │(100K)    │ │(100K)    │         │                            │
│  │  └─────────┘ └──────────┘ └──────────┘         │                            │
│  │  ┌─────────┐ ┌──────────┐ ┌──────────┐         │                            │
│  │  │Employees│ │POs       │ │PO Lines  │         │                            │
│  │  │(100K)   │ │(100K)    │ │(100K)    │         │                            │
│  │  └─────────┘ └──────────┘ └──────────┘         │                            │
│  │  ┌─────────┐ ┌──────────┐ ┌──────────┐         │                            │
│  │  │Contracts│ │Invoices  │ │GRNs      │         │                            │
│  │  │(100K)   │ │(100K)    │ │(100K)    │         │                            │
│  │  └─────────┘ └──────────┘ └──────────┘         │                            │
│  │  ┌─────────┐ ┌──────────┐ ┌──────────┐         │                            │
│  │  │Budgets  │ │Actuals   │ │Inventory │         │                            │
│  │  │(100K)   │ │(100K)    │ │(100K)    │         │                            │
│  │  └─────────┘ └──────────┘ └──────────┘         │                            │
│  │  ┌─────────┐ ┌──────────┐ ┌──────────┐         │                            │
│  │  │Sales    │ │Vendor    │ │(Future   │         │                            │
│  │  │Orders   │ │Perf.     │ │Sources)  │         │                            │
│  │  └─────────┘ └──────────┘ └──────────┘         │                            │
│  └────────────────────┬────────────────────────────┘                            │
│                       │                                                         │
│                       ▼                                                         │
│  ┌─────────────────────────────────────────────────┐                            │
│  │  🥉 BRONZE LAYER — Raw Ingestion (15 tables)    │ ← Automated daily 6AM     │
│  │  • Auto-ingest CSV/XLSX via Delta Live Tables   │                            │
│  │  • Metadata: _ingest_timestamp, _source_file    │                            │
│  │  • DQ: on_violation = DROP (reject corrupt)     │                            │
│  │  • Timeout: 30 min                              │                            │
│  └────────────────────┬────────────────────────────┘                            │
│                       │                                                         │
│                       ▼                                                         │
│  ┌─────────────────────────────────────────────────┐                            │
│  │  🥈 SILVER LAYER — Cleansing (15 tables + QT)   │                            │
│  │  • Type casting, null handling, standardization │                            │
│  │  • Derived fields:                              │                            │
│  │    - budget_variance_pct                        │                            │
│  │    - fulfillment_rate, acceptance_rate           │                            │
│  │    - days_to_pay, days_overdue                  │                            │
│  │    - contract_duration_days                     │                            │
│  │  • DQ: on_violation = QUARANTINE                │                            │
│  │  • Failed records → silver_quarantine table     │                            │
│  └────────────────────┬────────────────────────────┘                            │
│                       │                                                         │
│                       ▼                                                         │
│  ┌─────────────────────────────────────────────────┐                            │
│  │  🥇 GOLD LAYER — Star Schema                    │                            │
│  │  ┌────────────────────────────────────────┐     │                            │
│  │  │  10 DIMENSIONS (SCD Type 2)            │     │                            │
│  │  │  dim_date • dim_vendor • dim_project   │     │                            │
│  │  │  dim_material • dim_employee           │     │                            │
│  │  │  dim_geography • dim_contract          │     │                            │
│  │  │  dim_cost_center • dim_sector          │     │                            │
│  │  └────────────────────────────────────────┘     │                            │
│  │  ┌────────────────────────────────────────┐     │                            │
│  │  │  7 FACT TABLES                         │     │                            │
│  │  │  fact_purchase_orders (with YoY)       │     │                            │
│  │  │  fact_invoices (aging buckets)         │     │                            │
│  │  │  fact_goods_receipts (quality metrics) │     │                            │
│  │  │  fact_project_costs (CPI/EVM)          │     │                            │
│  │  │  fact_project_actuals                  │     │                            │
│  │  │  fact_vendor_performance (rankings)    │     │                            │
│  │  │  fact_contracts (risk flags)           │     │                            │
│  │  └────────────────────────────────────────┘     │                            │
│  └────────────────────┬────────────────────────────┘                            │
│                       │                                                         │
│                       ▼                                                         │
│  ┌─────────────────────────────────────────────────┐                            │
│  │  📊 SEMANTIC LAYER — 15 Materialized Cubes       │                            │
│  │  Procurement Spend • Vendor Performance          │                            │
│  │  Contract Status • Invoice Aging                 │                            │
│  │  Project Costs • Goods Receipt Quality           │                            │
│  │  Budget vs Actual • Sales Analysis               │                            │
│  │  Inventory Status • Spend by Sector              │                            │
│  │  Delivery Performance • Cost Variance            │                            │
│  │  Vendor Risk • Project Timeline                  │                            │
│  │  Procurement Efficiency                          │                            │
│  │  → Auto-refresh on schedule                      │                            │
│  └────────────────────┬────────────────────────────┘                            │
│                       │                                                         │
│                       ▼                                                         │
│  ┌─────────────────────────────────────────────────┐                            │
│  │  📈 BI & ANALYTICS                               │                            │
│  │  Power BI / Tableau / Databricks SQL             │                            │
│  │  • Real-time dashboards (< 5 sec refresh)       │                            │
│  │  • Self-service ad-hoc queries                  │                            │
│  │  • Automated alerts & notifications             │                            │
│  │  • Mobile-accessible KPI summaries              │                            │
│  └─────────────────────────────────────────────────┘                            │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Detailed Process Workflows

### 3.1 Procurement Lifecycle Workflow

```
START
  │
  ▼
┌─────────────────────────────────────────────────────────────────┐
│ STEP 1: PROJECT INITIATION & BUDGET ALLOCATION                  │
│                                                                 │
│ Data: dim_project, fact_project_costs                           │
│ Cube: cube_project_costs, cube_budget_vs_actual                 │
│                                                                 │
│ • Project created with sector, type, budget, WBS codes          │
│ • Budget allocated by cost type (Material, Labour, Equipment)   │
│ • Budget performance tracked: CPI = Budget / (Actual+Committed) │
│ • Automated alerts when CPI < 0.9 (over-budget risk)           │
└───────────────────────┬─────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────────┐
│ STEP 2: VENDOR SELECTION & PREQUALIFICATION                     │
│                                                                 │
│ Data: dim_vendor, fact_vendor_performance                       │
│ Cube: cube_vendor_performance, cube_vendor_risk                 │
│                                                                 │
│ • Vendor scorecards: Delivery, Quality, Commercial, HSE scores  │
│ • Auto-tier: STRATEGIC (≥4.0), PREFERRED (≥3.0), STANDARD      │
│ • Risk assessment: financial health, ISO, prequalification      │
│ • Historical performance trend analysis (period-over-period)    │
│ • Concentration risk: % of total spend per vendor               │
└───────────────────────┬─────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────────┐
│ STEP 3: CONTRACT MANAGEMENT                                     │
│                                                                 │
│ Data: dim_contract, fact_contracts                              │
│ Cube: cube_contract_status                                      │
│                                                                 │
│ • Contract creation with BOQ (Bill of Quantities)               │
│ • Change order tracking: cost_growth_pct monitored              │
│ • Risk flags:                                                   │
│   - COST_RISK: cost growth > 20%                               │
│   - SCHEDULE_RISK: OVERDUE or NEAR_EXPIRY (< 30 days)         │
│ • Value utilisation: items_total vs contract_value tracked      │
│ • Performance bond & penalty clause enforcement                 │
└───────────────────────┬─────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────────┐
│ STEP 4: PURCHASE ORDER PROCESSING                               │
│                                                                 │
│ Data: fact_purchase_orders, silver_purchase_orders               │
│ Cube: cube_procurement_spend, cube_procurement_efficiency       │
│                                                                 │
│ • PO creation with type classification:                         │
│   STANDARD(50%), BLANKET(20%), EMERGENCY(10%), FRAMEWORK(20%)   │
│ • Multi-currency support: USD, EUR, GBP, SAR                   │
│ • Approval routing by employee grade (L1=$10K → L6=unlimited)  │
│ • Spend trending: YoY growth % by vendor/sector/project         │
│ • Cycle time tracking: requisition-to-PO days                  │
└───────────────────────┬─────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────────┐
│ STEP 5: GOODS RECEIPT & QUALITY INSPECTION                      │
│                                                                 │
│ Data: fact_goods_receipts, silver_goods_receipts                │
│ Cube: cube_goods_receipt_quality, cube_delivery_performance     │
│                                                                 │
│ • GRN processing: qty received, accepted, rejected              │
│ • Quality metrics:                                              │
│   - Acceptance Rate = qty_accepted / qty_received × 100         │
│   - Defect Rate = qty_rejected / qty_received × 100             │
│   - Quality Tier: EXCELLENT(≥98%) / GOOD(≥95%) /               │
│     ACCEPTABLE(≥90%) / POOR(<90%)                               │
│ • On-time delivery tracking per vendor/material                 │
│ • Estimated rejection value: qty_rejected × unit_price          │
└───────────────────────┬─────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────────┐
│ STEP 6: INVOICE PROCESSING & ACCOUNTS PAYABLE                   │
│                                                                 │
│ Data: fact_invoices, silver_invoices                            │
│ Cube: cube_invoice_aging                                        │
│                                                                 │
│ • Invoice matching: 3-way match (PO, GRN, Invoice)             │
│ • Aging analysis by bucket:                                     │
│   CURRENT | 1-30 DAYS | 31-60 DAYS | 61-90 DAYS | 90+ DAYS    │
│ • Payment delay metrics: days_to_pay, days_overdue              │
│ • Overdue amount % and DSO (Days Sales Outstanding)             │
│ • Cash flow forecasting based on due date pipeline              │
└───────────────────────┬─────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────────┐
│ STEP 7: PROJECT COST CONTROL & EARNED VALUE                     │
│                                                                 │
│ Data: fact_project_costs, fact_project_actuals                  │
│ Cube: cube_project_costs, cube_budget_vs_actual,                │
│       cube_cost_variance, cube_project_timeline                 │
│                                                                 │
│ • Budget vs Actual at WBS level                                 │
│ • Earned Value metrics:                                         │
│   - CPI = Budget / Total Exposure                              │
│   - Variance % = (Budget - Actual) / Budget × 100              │
│   - Utilisation % = Actual / Budget × 100                      │
│ • Variance classification:                                      │
│   UNDER BUDGET | ON BUDGET | OVER BUDGET | CRITICAL OVERRUN    │
│ • Schedule tracking: days ahead/behind, milestone completion    │
└───────────────────────┬─────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────────┐
│ STEP 8: INVENTORY & MATERIAL MANAGEMENT                         │
│                                                                 │
│ Data: silver_inventory, silver_materials                        │
│ Cube: cube_inventory_status                                     │
│                                                                 │
│ • Stock levels: on-hand, reserved, available                    │
│ • Material categorization: 15 categories (Structural Steel,     │
│   Concrete, Pipes, Valves, Electrical, etc.)                    │
│ • Hazardous material tracking (25% of chemicals/fuel)           │
│ • Strategic item identification                                 │
│ • Turnover rate analysis per material/project                   │
└───────────────────────┬─────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────────┐
│ STEP 9: VENDOR PERFORMANCE EVALUATION                           │
│                                                                 │
│ Data: fact_vendor_performance, dim_vendor                       │
│ Cube: cube_vendor_performance, cube_vendor_risk                 │
│                                                                 │
│ • Periodic evaluation (quarterly/annual):                       │
│   - Delivery Score (0-100)                                     │
│   - Quality Score (0-100)                                      │
│   - Commercial Score (0-100)                                   │
│   - HSE Score (0-100)                                          │
│   - Overall Score (weighted composite)                         │
│ • Vendor ranking: dense rank within evaluation period           │
│ • Score trending: period-over-period improvement/decline        │
│ • Recommendations: CONTINUE / PROBATION / TERMINATE            │
└───────────────────────┬─────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────────┐
│ STEP 10: REPORTING & ANALYTICS                                  │
│                                                                 │
│ All 15 Semantic Cubes → BI Dashboards                          │
│                                                                 │
│ • Executive dashboard: Spend, CPI, vendor health at a glance   │
│ • Procurement dashboard: PO status, cycle times, savings       │
│ • Project dashboard: Budget health, schedule, milestones        │
│ • Finance dashboard: Invoice aging, cash flow, DSO              │
│ • Quality dashboard: Acceptance rates, defect trends            │
│ • Risk dashboard: Vendor risk, contract risk, budget risk       │
│                                                                 │
│ Delivery: Power BI, Tableau, Databricks SQL, or any ODBC tool  │
└─────────────────────────────────────────────────────────────────┘
```

---

### 3.2 Pipeline Orchestration Workflow

```
┌────────────────────────────────────────────────────────────────────────┐
│              DAILY AUTOMATED PIPELINE — 6:00 AM (Asia/Riyadh)         │
│              Max Runtime: 2 hours | Max Concurrent: 1                 │
├────────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  TASK 1: Unity Catalog Setup                                           │
│  ┌──────────────────────────────┐                                      │
│  │ uc_setup (5 min timeout)     │                                      │
│  │ • Create catalog if needed   │                                      │
│  │ • Create schemas             │                                      │
│  │ • Set permissions            │                                      │
│  └──────────────┬───────────────┘                                      │
│                 │                                                       │
│                 ▼                                                       │
│  TASK 2: Bronze Ingestion                                              │
│  ┌──────────────────────────────┐                                      │
│  │ bronze_pipeline (30 min)     │                                      │
│  │ • Ingest 15 CSV/XLSX files   │                                      │
│  │ • Add audit metadata         │                                      │
│  │ • DQ: DROP invalid records   │                                      │
│  │ • Photon-accelerated         │                                      │
│  │ • 1-4 auto-scaling workers   │                                      │
│  └──────────────┬───────────────┘                                      │
│                 │                                                       │
│                 ▼                                                       │
│  TASK 3: Silver Transformation                                         │
│  ┌──────────────────────────────┐                                      │
│  │ silver_pipeline (30 min)     │                                      │
│  │ • Type enforcement           │                                      │
│  │ • Null handling              │                                      │
│  │ • Derived field calculation  │                                      │
│  │ • DQ: QUARANTINE violations  │                                      │
│  │ • Standardization            │                                      │
│  └──────────────┬───────────────┘                                      │
│                 │                                                       │
│                 ▼                                                       │
│  TASK 4: Gold Star Schema                                              │
│  ┌──────────────────────────────┐                                      │
│  │ gold_pipeline (30 min)       │                                      │
│  │ • Build 10 SCD2 dimensions   │                                      │
│  │ • Build 7 fact tables        │                                      │
│  │ • Calculate KPIs             │                                      │
│  │ • Historical tracking        │                                      │
│  └──────────────┬───────────────┘                                      │
│                 │                                                       │
│       ┌─────────┴──────────────────────────────┐                       │
│       ▼         ▼         ▼         ▼          ▼                       │
│  TASKS 5-10: Semantic Cubes (PARALLEL, 10 min each)                    │
│  ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐               │
│  │Spend   │ │Vendor  │ │Contract│ │Invoice │ │Project │ ...            │
│  │Cube    │ │Perf    │ │Status  │ │Aging   │ │Costs   │               │
│  │cube_01 │ │cube_02 │ │cube_03 │ │cube_04 │ │cube_05 │               │
│  └────────┘ └────────┘ └────────┘ └────────┘ └────────┘               │
│                                                                        │
│  TOTAL ESTIMATED RUNTIME: 45-90 minutes                                │
│  OUTPUT: 15 bronze + 16 silver + 17 gold + 15 cubes = 63 tables       │
│                                                                        │
└────────────────────────────────────────────────────────────────────────┘
```

---

### 3.3 Data Quality Workflow

```
┌─────────────────────────────────────────────────────────────────┐
│                 DATA QUALITY MANAGEMENT FLOW                    │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  RAW DATA ARRIVES                                               │
│       │                                                         │
│       ▼                                                         │
│  ┌─────────────────────────────────────┐                        │
│  │ BRONZE DQ GATE — STRICT (DROP)      │                        │
│  │                                     │                        │
│  │ Rules:                              │                        │
│  │ • File format validation            │                        │
│  │ • Column count verification         │                        │
│  │ • Basic type compatibility          │                        │
│  │                                     │                        │
│  │ Action: REJECT corrupt rows         │                        │
│  └───────┬────────────┬────────────────┘                        │
│     PASS │            │ FAIL                                    │
│          │            │                                         │
│          ▼            ▼                                         │
│   bronze_*     ❌ Dropped                                       │
│   tables       (logged in audit)                                │
│          │                                                      │
│          ▼                                                      │
│  ┌─────────────────────────────────────┐                        │
│  │ SILVER DQ GATE — SOFT (QUARANTINE)  │                        │
│  │                                     │                        │
│  │ Rules:                              │                        │
│  │ • NOT NULL: vendor_id, project_id,  │                        │
│  │   po_id, material_id, employee_id   │                        │
│  │ • RANGE: performance_rating 0-5,    │                        │
│  │   approved_budget > 0, unit_price≥0 │                        │
│  │ • FORMAT: dates, currency codes     │                        │
│  │                                     │                        │
│  │ Action: QUARANTINE bad records      │                        │
│  └───────┬────────────┬────────────────┘                        │
│     PASS │            │ FAIL                                    │
│          │            │                                         │
│          ▼            ▼                                         │
│   silver_*     silver_quarantine                                │
│   tables       (for review & remediation)                       │
│          │                                                      │
│          ▼                                                      │
│  ┌─────────────────────────────────────┐                        │
│  │ GOLD — CONFORMANCE & ENRICHMENT     │                        │
│  │                                     │                        │
│  │ • SCD2 merge logic                  │                        │
│  │ • Referential integrity via JOINs   │                        │
│  │ • KPI derivation validation         │                        │
│  │ • _is_current flag management       │                        │
│  └─────────────────────────────────────┘                        │
│                                                                 │
│  OUTCOME: 100% of Gold data is validated,                       │
│  traceable, and historically versioned                          │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 4. Key Performance Indicators Tracked

### 4.1 Financial KPIs
| KPI | Formula | Target | Alert Threshold |
|-----|---------|--------|-----------------|
| Total Procurement Spend | SUM(po_value) by period | Within budget | > 110% of budget |
| YoY Spend Growth | (Current - Prior) / Prior × 100 | < 15% | > 20% |
| Invoice Days Outstanding | AVG(datediff(today, due_date)) | < 30 days | > 60 days |
| Overdue Invoice % | Overdue Amount / Total Amount | < 5% | > 15% |
| Cost Performance Index | Budget / (Actual + Committed) | > 1.0 | < 0.9 |
| Budget Variance % | (Budget - Actual) / Budget × 100 | > 0% | < -10% |

### 4.2 Supply Chain KPIs
| KPI | Formula | Target | Alert Threshold |
|-----|---------|--------|-----------------|
| On-Time Delivery % | On-time GRNs / Total GRNs | > 95% | < 85% |
| Material Acceptance Rate | qty_accepted / qty_received × 100 | > 98% | < 90% |
| Defect Rate % | qty_rejected / qty_received × 100 | < 2% | > 10% |
| Vendor Overall Score | Weighted avg (Del + Qual + Comm + HSE) | > 80/100 | < 60/100 |
| Procurement Cycle Time | PO date - Requisition date | < 5 days | > 15 days |

### 4.3 Project KPIs
| KPI | Formula | Target | Alert Threshold |
|-----|---------|--------|-----------------|
| Budget Utilisation % | Actual / Budget × 100 | 85-100% | > 100% |
| Contract Cost Growth % | (Revised - Original) / Original × 100 | < 5% | > 20% |
| Schedule Variance | Planned vs Actual completion days | On-track | > 30 days late |
| Contract Utilisation % | Items used / Items allocated × 100 | > 80% | < 50% |

---

## 5. Sector-Specific Applications

### 5.1 Civil Construction
- Track material costs (structural steel, concrete) by project phase
- Monitor subcontractor performance and delivery quality
- Budget vs actual at WBS level for highway, building, bridge projects
- Environmental clearance and compliance tracking

### 5.2 Oil & Gas (EPC)
- Pipeline/refinery/drilling project cost control
- Hazardous material tracking and HSE compliance
- Vendor prequalification for safety-critical suppliers
- Long lead time material planning (valves, instruments)

### 5.3 Power & Infrastructure
- Thermal/solar/wind project portfolio management
- Equipment procurement lifecycle tracking
- Multi-currency procurement across global vendors
- Substation and water treatment project timelines

---

## 6. Integration Points

| System | Integration Type | Data Flow |
|--------|-----------------|-----------|
| ERP (SAP/Oracle) | CSV/XLSX export → Bronze | Source → Lakehouse |
| Project Controls (Primavera) | CSV export → Bronze | Source → Lakehouse |
| Quality Management System | CSV export → Bronze | Source → Lakehouse |
| Power BI / Tableau | ODBC/JDBC on Semantic Layer | Lakehouse → BI |
| API Layer (.NET) | REST API on Commerce entities | Lakehouse ↔ App |
| Databricks SQL | Direct query on Gold/Cubes | Lakehouse → Users |

---

*Document Version: 1.0 | Generated: April 2026*
