# Construction & Oil/Gas Procurement Lakehouse
## Databricks | Medallion Architecture | Unity Catalog | Star Schema | 15 Live Cubes

---

## What This Project Builds

A **production-grade Databricks Lakehouse** for an EPC (Engineering, Procurement & Construction) company covering:

| Domain | Sectors |
|--------|---------|
| Civil construction | Buildings, highways, bridges, apartments, tunnels |
| Oil & Gas | Refineries, pipelines, LNG terminals, drilling |
| Power | Thermal, solar, wind, substation construction |
| Infrastructure | Ports, water treatment, rail |

### Scope at a Glance

| Layer | Tables | Records |
|-------|--------|---------|
| Bronze (raw ingestion) | 15 | 100K each = 1.5M total |
| Silver (cleansed) | 15 + DQ quarantine | ~1.5M |
| Gold — Dimensions (SCD2) | 10 | Full history |
| Gold — Facts (star schema) | 6 | ~1.5M joined |
| Semantic — Data cubes | 15 materialized views | Auto-refresh |

---

## Quick Start

### 1. Prerequisites

```bash
pip install faker pandas openpyxl numpy databricks-sdk
databricks --version   # Databricks CLI must be installed
```

### 2. Generate all test data (local machine, ~10 minutes)

```bash
cd data_generators
python 00_master_generator.py
# Output: 15 Excel files in data/raw/, each with 100,000 rows
```

### 3. Deploy infrastructure

```bash
# Set your Databricks workspace URL and token
export DATABRICKS_HOST=https://<your-workspace>.azuredatabricks.net
export DATABRICKS_TOKEN=<your-pat-token>

# Upload test data to ADLS Volume
for f in data/raw/*.xlsx; do
    databricks fs cp "$f" dbfs:/Volumes/procurement_dev/bronze/raw_files/$(basename "$f")
done

# Run Unity Catalog setup (as admin)
databricks workspace import databricks/setup/00_unity_catalog_setup.py
```

### 4. Run the 3 demo pipelines

```bash
# Deploy using Databricks Asset Bundles
databricks bundle deploy --target dev

# Run the full workflow (Bronze → Silver → Gold → Cubes)
databricks bundle run procurement_demo_workflow
```

### 5. Monitor progress

- **Databricks UI**: Workflows → procurement_demo_workflow
- **DLT Pipelines**: Delta Live Tables → procurement_*_pipeline
- **Validation**: Check logs for "ALL VALIDATIONS PASSED"

---

## Architecture

```
Data Sources (Excel/ADLS)
         │
         ▼ Pipeline 1 (DLT)
   ┌─────────────┐
   │   BRONZE    │  15 raw tables · Auto Loader · schema inference
   │  (raw data) │  metadata: _ingestion_ts, _source_file, _batch_id
   └──────┬──────┘
          │
          ▼ Pipeline 2 (DLT)
   ┌─────────────┐
   │   SILVER    │  15 clean tables · DQ expectations · quarantine
   │  (cleansed) │  SCD2 prep · null handling · type enforcement
   └──────┬──────┘
          │
          ▼ Pipeline 3 (DLT)
   ┌─────────────────────────────────────────┐
   │               GOLD                      │
   │  10 Dimensions (SCD2)                   │
   │  dim_vendor, dim_project, dim_material, │
   │  dim_contract, dim_employee, dim_date,  │
   │  dim_cost_center, dim_geography,        │
   │  dim_currency, dim_sector               │
   │                                         │
   │  6 Fact Tables (star schema)            │
   │  fact_purchase_orders                   │
   │  fact_contracts                         │
   │  fact_invoices                          │
   │  fact_project_costs                     │
   │  fact_goods_receipts                    │
   │  fact_sales                             │
   └──────┬──────────────────────────────────┘
          │
          ▼ Auto-refresh (Databricks SQL)
   ┌─────────────────────────────────────────┐
   │            SEMANTIC (15 Cubes)          │
   │  cube_procurement_spend                 │
   │  cube_vendor_performance                │
   │  cube_contract_performance              │
   │  cube_project_cost_variance (EVM)       │
   │  cube_po_cycle_time                     │
   │  cube_invoice_aging                     │
   │  cube_material_consumption              │
   │  cube_category_spend                    │
   │  cube_budget_utilization                │
   │  cube_sales_revenue                     │
   │  cube_procurement_savings               │
   │  cube_supplier_risk                     │
   │  cube_milestone_cost                    │
   │  cube_inventory_turnover                │
   │  cube_og_equipment (O&G specific)       │
   └─────────────────────────────────────────┘
          │
          ▼ DirectLake
   Power BI / Tableau Dashboards
```

---

## Star Schema Key Relationships

```
              dim_date
                 │
                 │ order_date_key
                 │
dim_vendor ──────┤              dim_material
    │            │                   │
    └──────► fact_purchase_orders ◄──┘
                 │
                 │ project_key
                 │
            dim_project ──────► fact_project_costs
                 │                    │
                 │              dim_cost_center
                 │
            dim_contract ─────► fact_contracts
                 │
            dim_employee (buyer, approver, contract_manager)
            dim_sector
            dim_geography
            dim_currency
```

---

## Data Cubes — Business KPIs

| Cube | Key KPIs |
|------|----------|
| Procurement Spend | Total spend, on-time delivery %, rejection rate |
| Vendor Performance | Composite score, delivery/quality/HSE ratings |
| Contract Performance | Cost growth %, schedule overrun, milestone status |
| Project Cost Variance | CPI, SPI, EAC, VAC (Earned Value Management) |
| PO Cycle Time | Avg days requisition→PO→delivery, percentiles |
| Invoice Aging | AP aging buckets, cash flow forecast 30/60/90 days |
| Material Consumption | Stock levels, wastage %, reorder alerts |
| Category Spend | Spend concentration, price competitiveness |
| Budget Utilization | Burn rate, months to exhaustion, contingency used |
| Sales Revenue | Product-level AND item-level revenue, margins |
| Procurement Savings | Negotiated savings vs benchmark |
| Supplier Risk | Risk score per vendor, spend at risk |
| Milestone Cost | S-curve, planned vs actual payments |
| Inventory Turnover | DIO, slow-moving stock, obsolete items |
| O&G Equipment | Long-lead items, commissioning readiness % |

---

## VS Code Copilot Integration

The file `.github/copilot-instructions.md` contains complete instructions for GitHub Copilot.

### How to use with Copilot:

1. Open VS Code in the project root
2. GitHub Copilot will automatically read `.github/copilot-instructions.md`
3. Open any generator file and ask Copilot:
   - *"Generate `02_gen_projects.py` following the same pattern as `01_gen_vendors.py`"*
   - *"Complete the silver pipeline for `clean_purchase_orders` with all DQ rules"*
   - *"Generate the full `fact_contracts` DLT table with all dimension joins"*
   - *"Write `cube_contract_performance.sql` as a materialized view"*

### Copilot Chat prompts (copy-paste ready):

```
# Generate a specific Silver table with all rules:
"Using the project's copilot-instructions.md as your guide, generate the complete
02_silver_pipeline.py including the clean_purchase_orders DLT table with all
expect_or_drop and expect_or_quarantine rules, SCD2 dedup, and silver metadata."

# Generate a Gold dimension:
"Generate the complete dim_project SCD2 dimension table for the gold DLT pipeline,
following the dim_vendor pattern in 03_gold_pipeline.py."

# Generate a specific cube:
"Generate cube_project_cost_variance.sql as a Databricks SQL Materialized View
with full Earned Value Management metrics (CPI, SPI, EAC, VAC, TCPI)."

# Validate a generator:
"Review 01_gen_vendors.py and check that all business rules from copilot-instructions.md
are correctly implemented. Point out any missing validations."
```

---

## File Inventory

| File | Purpose | Auto-generated by Copilot |
|------|---------|--------------------------|
| `data_generators/00_master_generator.py` | Runs all 15 generators | Provided |
| `data_generators/01_gen_vendors.py` | Vendor master (example) | Provided |
| `data_generators/02–15_gen_*.py` | Remaining 14 generators | ✅ Copilot |
| `databricks/setup/00_unity_catalog_setup.py` | UC catalogs/schemas/grants | ✅ Copilot |
| `databricks/bronze/01_bronze_pipeline.py` | DLT Pipeline 1 (15 tables) | ✅ Copilot |
| `databricks/silver/02_silver_pipeline.py` | DLT Pipeline 2 (15 tables) | ✅ Copilot |
| `databricks/gold/03_gold_pipeline.py` | DLT Pipeline 3 (16 tables) | ✅ Copilot |
| `databricks/semantic/04_cube_*.sql` | 15 materialized view cubes | ✅ Copilot |
| `databricks/pipelines/*.json` | DLT pipeline configs | ✅ Copilot |
| `databricks/orchestration/workflow_demo.json` | Full workflow config | ✅ Copilot |
| `tests/test_bronze_counts.py` | Validation assertions | ✅ Copilot |

---

## Unity Catalog Structure

```
procurement_dev (catalog)
├── bronze (schema)         ← 15 raw_* tables
├── silver (schema)         ← 15 clean_* tables + dq_quarantine + dq_metrics
├── gold (schema)           ← 10 dim_* + 6 fact_* tables
├── semantic (schema)       ← 15 cube_* materialized views
└── dq (schema)             ← data quality logs

procurement_staging (catalog)  ← same structure
procurement_prod    (catalog)  ← same structure
```

---

## Troubleshooting

| Issue | Fix |
|-------|-----|
| Generator fails with `ModuleNotFoundError` | `pip install faker pandas openpyxl numpy` |
| Excel file opens empty | Check `data/raw/` directory exists |
| DLT pipeline fails on read | Verify xlsx files are in Volume path |
| Unity Catalog permission denied | Run setup script as admin user |
| Cube refresh fails | Ensure gold pipeline ran successfully first |
| `record_count < 95000` in tests | Re-run generator with `--records 100000` |

---

## Performance Notes

- Generator runtime: ~8–15 min on standard laptop (15 × 100K rows)
- Bronze pipeline: ~5 min (serverless, 15 tables parallel)
- Silver pipeline: ~8 min (DQ checks + dedup)
- Gold pipeline: ~12 min (SCD2 merges + 6 large joins)
- Cube refresh: ~3 min (15 materialized views parallel)
- **Total demo run time: ~30–40 minutes**

---

*Project template for VS Code + GitHub Copilot*
*Stack: Databricks 14.3 LTS | Delta Lake 3.0 | Unity Catalog | DLT | Lakeflow*
