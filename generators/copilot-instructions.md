# GitHub Copilot Instructions — Construction & Oil/Gas Procurement Lakehouse
# Databricks Medallion Architecture | Unity Catalog | Star Schema | 15 Live Cubes

---

## WHO YOU ARE (COPILOT ROLE)

You are a senior data engineer and domain expert for a large-scale EPC (Engineering, Procurement & Construction) company that operates across:
- **Civil construction**: Buildings, highways, bridges, tunnels, apartments, commercial complexes
- **Oil & Gas**: Onshore/offshore drilling rigs, pipelines, refineries, LNG terminals, tank farms
- **Power plants**: Thermal, hydro, solar, wind, substation construction
- **Infrastructure**: Water treatment plants, ports, airports, rail

Your job is to generate ALL code, data, schemas, pipelines, and orchestration for a **production-grade Databricks Lakehouse** that manages end-to-end procurement, vendor management, project costing, contract management, and sales analytics. Every piece of code you write must be complete, runnable, and production-ready.

---

## ABSOLUTE RULES — NEVER VIOLATE THESE

1. **Every table must have exactly 100,000 records** in test data. Use Python + Faker + random with a fixed seed for reproducibility.
2. **Every pipeline must be a Delta Live Tables (DLT) pipeline** using `@dlt.table` and `@dlt.expect_or_quarantine` decorators.
3. **All tables must use Unity Catalog three-part naming**: `catalog.schema.table_name`.
4. **Star schema Gold layer is non-negotiable** — every fact table must have at least 4 foreign keys to dimension tables.
5. **Every data cube (semantic layer) must be a Databricks SQL Materialized View** that auto-refreshes on pipeline completion.
6. **All dimension tables use SCD Type 2** — include `eff_start_date`, `eff_end_date`, `is_current`, `surrogate_key`.
7. **Bronze layer stores raw data as-is** — only add `_ingestion_timestamp`, `_source_file`, `_batch_id` metadata columns.
8. **Silver layer enforces data quality** — use DLT expectations, quarantine failed records, log DQ metrics.
9. **All monetary values in USD** — include a `dim_currency` table for exchange rate conversion.
10. **Every notebook must have a header docstring** explaining purpose, inputs, outputs, dependencies.
11. **Never truncate code** — generate complete, working files. If a file is long, complete it fully.
12. **Test data generators must be self-contained Python scripts** — no external dependencies beyond `pip install faker pandas openpyxl numpy`.

---

## PROJECT STRUCTURE (CREATE THIS EXACTLY)

```
procurement-lakehouse/
├── .github/
│   └── copilot-instructions.md          ← THIS FILE
├── data_generators/
│   ├── 00_master_generator.py           ← Runs all generators in sequence
│   ├── 01_gen_vendors.py                ← 100K vendor records → vendors.xlsx
│   ├── 02_gen_projects.py               ← 100K project records → projects.xlsx
│   ├── 03_gen_materials.py              ← 100K material/item records → materials.xlsx
│   ├── 04_gen_employees.py              ← 100K employee records → employees.xlsx
│   ├── 05_gen_purchase_orders.py        ← 100K PO headers → purchase_orders.xlsx
│   ├── 06_gen_po_line_items.py          ← 100K PO line items → po_line_items.xlsx
│   ├── 07_gen_contracts.py              ← 100K contracts → contracts.xlsx
│   ├── 08_gen_contract_items.py         ← 100K contract items → contract_items.xlsx
│   ├── 09_gen_invoices.py               ← 100K invoices → invoices.xlsx
│   ├── 10_gen_goods_receipts.py         ← 100K GRN records → goods_receipts.xlsx
│   ├── 11_gen_project_budgets.py        ← 100K budget lines → project_budgets.xlsx
│   ├── 12_gen_project_actuals.py        ← 100K actual cost entries → project_actuals.xlsx
│   ├── 13_gen_sales_orders.py           ← 100K sales orders → sales_orders.xlsx
│   ├── 14_gen_inventory.py              ← 100K inventory movements → inventory.xlsx
│   └── 15_gen_vendor_performance.py     ← 100K performance records → vendor_perf.xlsx
├── data/
│   └── raw/                             ← Generated Excel files land here (15 files)
├── databricks/
│   ├── setup/
│   │   ├── 00_unity_catalog_setup.py    ← Create catalogs, schemas, volumes
│   │   └── 00_cluster_config.json       ← Cluster configuration
│   ├── bronze/                          ← DLT pipeline 1: Raw ingestion
│   │   ├── 01_bronze_pipeline.py        ← All bronze DLT tables in one pipeline
│   │   └── 01_bronze_autoloader.py      ← Auto Loader config per source
│   ├── silver/                          ← DLT pipeline 2: Cleansing & conforming
│   │   ├── 02_silver_vendors.py
│   │   ├── 02_silver_projects.py
│   │   ├── 02_silver_materials.py
│   │   ├── 02_silver_purchase_orders.py
│   │   ├── 02_silver_contracts.py
│   │   ├── 02_silver_invoices.py
│   │   ├── 02_silver_project_costs.py
│   │   ├── 02_silver_sales.py
│   │   └── 02_silver_pipeline.py        ← Master silver DLT pipeline
│   ├── gold/                            ← DLT pipeline 3: Star schema build
│   │   ├── dimensions/
│   │   │   ├── 03_dim_vendor.py
│   │   │   ├── 03_dim_project.py
│   │   │   ├── 03_dim_material.py
│   │   │   ├── 03_dim_contract.py
│   │   │   ├── 03_dim_employee.py
│   │   │   ├── 03_dim_date.py
│   │   │   ├── 03_dim_cost_center.py
│   │   │   ├── 03_dim_geography.py
│   │   │   ├── 03_dim_currency.py
│   │   │   └── 03_dim_sector.py
│   │   ├── facts/
│   │   │   ├── 03_fact_purchase_orders.py
│   │   │   ├── 03_fact_contracts.py
│   │   │   ├── 03_fact_invoices.py
│   │   │   ├── 03_fact_project_costs.py
│   │   │   ├── 03_fact_goods_receipts.py
│   │   │   └── 03_fact_sales.py
│   │   └── 03_gold_pipeline.py          ← Master gold DLT pipeline
│   ├── semantic/                        ← 15 data cubes as materialized views
│   │   ├── 04_cube_procurement_spend.sql
│   │   ├── 04_cube_vendor_performance.sql
│   │   ├── 04_cube_contract_performance.sql
│   │   ├── 04_cube_project_cost_variance.sql
│   │   ├── 04_cube_po_cycle_time.sql
│   │   ├── 04_cube_invoice_aging.sql
│   │   ├── 04_cube_material_consumption.sql
│   │   ├── 04_cube_category_spend.sql
│   │   ├── 04_cube_budget_utilization.sql
│   │   ├── 04_cube_sales_revenue.sql
│   │   ├── 04_cube_procurement_savings.sql
│   │   ├── 04_cube_supplier_risk.sql
│   │   ├── 04_cube_milestone_cost.sql
│   │   ├── 04_cube_inventory_turnover.sql
│   │   └── 04_cube_og_equipment.sql
│   ├── pipelines/
│   │   ├── pipeline_bronze.json         ← DLT pipeline config for bronze
│   │   ├── pipeline_silver.json         ← DLT pipeline config for silver
│   │   └── pipeline_gold.json           ← DLT pipeline config for gold
│   └── orchestration/
│       ├── workflow_full_pipeline.json  ← Full end-to-end workflow
│       └── workflow_demo.json           ← Demo 3-pipeline workflow
└── tests/
    ├── test_bronze_counts.py
    ├── test_silver_quality.py
    └── test_gold_schema.py
```

---

## TECH STACK

```
Language:          Python 3.10+ | SQL (Spark SQL)
Platform:          Azure Databricks (Runtime 14.3 LTS ML)
Storage:           Azure Data Lake Storage Gen2 (ADLS2)
Table Format:      Delta Lake 3.0
Pipeline Engine:   Delta Live Tables (DLT) / Lakeflow Declarative Pipelines
Governance:        Unity Catalog
Orchestration:     Databricks Workflows (Lakeflow Jobs)
Semantic Layer:    Databricks SQL Materialized Views
BI/Reporting:      Power BI (DirectLake mode)
Data Generation:   Python + Faker 24+ + pandas + openpyxl + numpy
CI/CD:             Databricks Asset Bundles (DAB) + GitHub Actions
Testing:           pytest + databricks-sdk
```

---

## DOMAIN DATA MODEL — FULL STAR SCHEMA DESIGN

### DOMAIN 1: PROCUREMENT DOMAIN

#### FACT: `fact_purchase_orders`
```
Grain: One row per PO line item
Measures: quantity_ordered, quantity_received, unit_price, line_amount,
          tax_amount, discount_amount, total_amount, outstanding_amount,
          days_to_delivery, price_variance_pct
Foreign Keys:
  → dim_vendor (vendor_key)
  → dim_material (material_key)
  → dim_project (project_key)
  → dim_contract (contract_key)
  → dim_employee (buyer_key)        ← the buyer who raised the PO
  → dim_cost_center (cost_center_key)
  → dim_date (order_date_key)
  → dim_date (required_date_key)
  → dim_date (delivery_date_key)
  → dim_currency (currency_key)
  → dim_geography (delivery_site_key)
  → dim_sector (sector_key)
Natural Keys: po_number, po_line_number
Degenerate Dims: po_status, approval_status, delivery_status, priority_flag,
                 po_type (STANDARD/BLANKET/EMERGENCY/FRAMEWORK)
```

#### FACT: `fact_contracts`
```
Grain: One row per contract line item (BOQ item)
Measures: contracted_quantity, contracted_unit_rate, contracted_amount,
          revised_quantity, revised_amount, variation_amount,
          paid_to_date, retention_held, retention_released,
          completion_pct, milestone_value
Foreign Keys:
  → dim_vendor (contractor_key)
  → dim_project (project_key)
  → dim_material (scope_item_key)
  → dim_employee (contract_manager_key)
  → dim_date (award_date_key)
  → dim_date (start_date_key)
  → dim_date (completion_date_key)
  → dim_cost_center (cost_center_key)
  → dim_sector (sector_key)
Natural Keys: contract_number, boq_item_number
Degenerate Dims: contract_type (LUMP_SUM/REMEASURE/COST_PLUS/TURNKEY),
                 contract_status, payment_terms, bond_type, insurance_status
```

#### FACT: `fact_invoices`
```
Grain: One row per invoice line item
Measures: invoice_quantity, invoice_unit_price, invoice_line_amount,
          tax_amount, retention_deducted, advance_deducted,
          net_payable, approved_amount, disputed_amount,
          days_outstanding, early_payment_discount
Foreign Keys:
  → dim_vendor (vendor_key)
  → dim_project (project_key)
  → dim_material (material_key)
  → dim_date (invoice_date_key)
  → dim_date (due_date_key)
  → dim_date (payment_date_key)
  → dim_employee (approver_key)
  → dim_currency (currency_key)
Natural Keys: invoice_number, invoice_line_number
Degenerate Dims: invoice_status, payment_method, invoice_type (MATERIAL/SERVICE/PROGRESS)
```

#### FACT: `fact_goods_receipts`
```
Grain: One row per GRN line item
Measures: quantity_received, quantity_accepted, quantity_rejected,
          received_value, rejection_value, unit_price_at_receipt
Foreign Keys:
  → dim_vendor (vendor_key)
  → dim_material (material_key)
  → dim_project (project_key)
  → dim_date (receipt_date_key)
  → dim_employee (inspector_key)
  → dim_geography (site_key)
Natural Keys: grn_number, grn_line_number
```

---

### DOMAIN 2: PROJECT COST DOMAIN

#### FACT: `fact_project_costs`
```
Grain: One row per cost transaction per WBS element per period
Measures: budget_amount, committed_amount, actual_amount,
          forecast_amount, variance_amount, variance_pct,
          earned_value, cost_performance_index,
          schedule_performance_index, estimate_at_completion,
          estimate_to_complete, variance_at_completion
Foreign Keys:
  → dim_project (project_key)
  → dim_cost_center (wbs_key)
  → dim_material (cost_item_key)
  → dim_vendor (vendor_key)           ← vendor incurring cost
  → dim_employee (cost_owner_key)
  → dim_date (period_key)
  → dim_sector (sector_key)
Natural Keys: cost_transaction_id, wbs_code, period_month
Degenerate Dims: cost_type (LABOUR/MATERIAL/EQUIPMENT/SUBCONTRACT/OVERHEAD),
                 cost_status, change_order_number
```

---

### DOMAIN 3: SALES & REVENUE DOMAIN

#### FACT: `fact_sales`
```
Grain: One row per sales order line item
Measures: quantity_sold, unit_selling_price, revenue_amount,
          cost_of_goods, gross_margin, gross_margin_pct,
          discount_given, net_revenue, tax_collected,
          quantity_at_product_level, quantity_at_item_level
Foreign Keys:
  → dim_project (project_key)         ← project being sold/billed
  → dim_material (product_key)        ← product/scope item sold
  → dim_vendor (customer_key)         ← reuse vendor dim for customer
  → dim_employee (sales_person_key)
  → dim_date (sale_date_key)
  → dim_date (billing_date_key)
  → dim_geography (sales_region_key)
  → dim_sector (sector_key)
  → dim_currency (currency_key)
Natural Keys: sales_order_number, sales_order_line
```

---

### DIMENSION TABLES (ALL SCD TYPE 2)

#### `dim_vendor`
```
Surrogate: vendor_key (BIGINT, auto-increment)
Natural:   vendor_id (VARCHAR)
Attributes:
  vendor_name, vendor_legal_name, vendor_code
  vendor_type (SUBCONTRACTOR/SUPPLIER/CONSULTANT/LABOUR_CONTRACTOR/OEM)
  sector_specialization (CIVIL/OIL_GAS/POWER/MULTI)
  country, state, city, postal_code, address_line1, address_line2
  registration_number, vat_number, tax_id
  bank_name, bank_account, swift_code
  payment_terms_days (30/45/60/90)
  credit_limit_usd
  performance_rating (1.0–5.0)
  risk_category (LOW/MEDIUM/HIGH/BLACKLISTED)
  prequalification_status (APPROVED/PENDING/EXPIRED/REJECTED)
  prequalification_expiry_date
  iso_certified (BOOLEAN)
  hse_rating (1–5)
  minority_owned (BOOLEAN)
  small_business (BOOLEAN)
  contact_name, contact_email, contact_phone
  -- SCD2 columns:
  eff_start_date, eff_end_date, is_current, record_hash
```

#### `dim_project`
```
Surrogate: project_key (BIGINT)
Natural:   project_id (VARCHAR)
Attributes:
  project_name, project_code
  project_type (BUILDING/HIGHWAY/APARTMENT/BRIDGE/REFINERY/
                PIPELINE/DRILLING/LNG/POWER_THERMAL/
                POWER_SOLAR/POWER_WIND/SUBSTATION/PORT/WATER_TREATMENT)
  sector (CIVIL_CONSTRUCTION/OIL_GAS/POWER/INFRASTRUCTURE)
  sub_sector (details per sector)
  client_name, client_id
  project_manager_id, project_director_id
  country, state, city, site_latitude, site_longitude
  contract_value_usd
  approved_budget_usd
  project_status (BIDDING/AWARDED/MOBILIZATION/CONSTRUCTION/COMMISSIONING/HANDOVER/CLOSED)
  start_date, planned_completion_date, actual_completion_date
  phase (FEED/DETAILED_DESIGN/PROCUREMENT/CONSTRUCTION/COMMISSIONING)
  priority (CRITICAL/HIGH/MEDIUM/LOW)
  wbs_level_1, wbs_level_2, wbs_level_3
  risk_level (LOW/MEDIUM/HIGH/EXTREME)
  safety_classification
  environmental_clearance (BOOLEAN)
  -- SCD2 columns
```

#### `dim_material`
```
Surrogate: material_key (BIGINT)
Natural:   material_id (VARCHAR)
Attributes:
  material_code, material_name, material_description
  -- CRITICAL: supports both PRODUCT level and ITEM level
  material_level (PRODUCT/ITEM/COMPONENT/ASSEMBLY)
  parent_material_id          ← for item-level rollup to product level
  product_category (STRUCTURAL_STEEL/CONCRETE/PIPES/VALVES/
                    ELECTRICAL/INSTRUMENTS/CIVIL/MECHANICAL/
                    CHEMICALS/SAFETY/IT/SERVICES/EQUIPMENT/FUEL/CONSUMABLES)
  sub_category
  commodity_code (UNSPSC code)
  hsn_code (Harmonized System)
  unit_of_measure (KG/MT/NOS/LTR/SQM/CUM/RM/SET/LOT/HOURS/DAYS)
  standard_unit_price_usd
  currency_code
  lead_time_days
  minimum_order_quantity
  shelf_life_days
  hazardous_material (BOOLEAN)
  requires_inspection (BOOLEAN)
  strategic_item (BOOLEAN)     ← long lead, critical path items
  local_content (BOOLEAN)
  -- SCD2 columns
```

#### `dim_contract`
```
Surrogate: contract_key (BIGINT)
Natural:   contract_id (VARCHAR)
Attributes:
  contract_number, contract_title
  contract_type (LUMP_SUM/REMEASURE/COST_PLUS/TURNKEY/EPC/EPCM/PMC)
  contract_category (MAIN_CONTRACT/SUBCONTRACT/SUPPLY/SERVICE/FRAMEWORK)
  scope_of_work
  governing_law, jurisdiction
  dispute_resolution
  mobilization_advance_pct
  retention_pct
  defect_liability_period_months
  parent_contract_id           ← for subcontracts under main contract
  -- SCD2 columns
```

#### `dim_employee`
```
Surrogate: employee_key (BIGINT)
Natural:   employee_id (VARCHAR)
Attributes:
  employee_name, employee_code
  department (PROCUREMENT/ENGINEERING/FINANCE/PROJECTS/HSE/LEGAL/CONTRACTS)
  designation, grade
  role (BUYER/CONTRACT_MANAGER/PROJECT_MANAGER/ENGINEER/DIRECTOR/APPROVER)
  approval_limit_usd
  cost_center_id
  -- SCD2 columns
```

#### `dim_date`
```
Surrogate:  date_key (INT, format YYYYMMDD)
Attributes:
  full_date, day_of_week, day_name, day_of_month, day_of_quarter, day_of_year
  week_of_year, week_start_date, week_end_date
  month_number, month_name, month_short_name, month_start_date, month_end_date
  quarter_number, quarter_name, quarter_start_date, quarter_end_date
  year_number, fiscal_year, fiscal_quarter, fiscal_month
  is_weekend, is_holiday, is_working_day
  holiday_name
  -- Construction-specific:
  is_monsoon_season            ← affects site productivity
  season (Q1_DRY/Q2_PRE_MONSOON/Q3_MONSOON/Q4_POST_MONSOON)
```

#### `dim_cost_center` (doubles as WBS dimension)
```
Surrogate: cost_center_key (BIGINT)
Natural:   cost_center_id (VARCHAR)
Attributes:
  wbs_code, wbs_description
  wbs_level (1/2/3/4)
  parent_wbs_code
  cost_center_type (DIRECT/INDIRECT/OVERHEAD/CONTINGENCY)
  discipline (CIVIL/STRUCTURAL/MECHANICAL/ELECTRICAL/INSTRUMENTATION/
              PIPING/HVAC/PLUMBING/PAINTING/INSULATION/HSE/ADMIN)
  -- SCD2 columns
```

#### `dim_geography`
```
Surrogate: geography_key (BIGINT)
Attributes:
  country_code, country_name, region, state, city, district
  climate_zone, terrain_type
  infrastructure_quality (1–5)
  logistics_difficulty (LOW/MEDIUM/HIGH/EXTREME)
```

#### `dim_currency`
```
Surrogate: currency_key (BIGINT)
Attributes:
  currency_code, currency_name
  exchange_rate_to_usd
  rate_date
  rate_source
```

#### `dim_sector`
```
Surrogate: sector_key (BIGINT)
Attributes:
  sector_code, sector_name
  sub_sector_code, sub_sector_name
  regulatory_body
  safety_standards (ISO_45001/OSHA/API/IEC/NFPA)
```

---

## DATA GENERATOR INSTRUCTIONS (COPILOT: GENERATE ALL FILES)

### MASTER GENERATOR — `data_generators/00_master_generator.py`

Generate this file first. It must:
1. Install dependencies: `pip install faker pandas openpyxl numpy`
2. Import and run all 15 generators in dependency order
3. Print progress and record counts for each file
4. Save all files to `../data/raw/` directory
5. Print final summary of all 15 files with row counts and file sizes

### GENERATOR FILE TEMPLATE (apply to ALL 15 generators):

```python
"""
Generator: [NAME]
Output: data/raw/[filename].xlsx
Records: 100,000
Seed: 42 (for reproducibility)
Dependencies: [list parent generators if FK references needed]

Schema:
  col1 (dtype): description
  col2 (dtype): description
  ...

Business Rules:
  - Rule 1
  - Rule 2
"""

import pandas as pd
import numpy as np
from faker import Faker
from faker.providers import company, address, person, phone_number
import random
from datetime import datetime, timedelta
import os

fake = Faker(['en_US', 'en_GB', 'ar_SA', 'en_AU'])  # Multi-locale for global company
Faker.seed(42)
np.random.seed(42)
random.seed(42)

N = 100_000
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'raw')
```

---

### GENERATOR 01: `01_gen_vendors.py` — VENDOR MASTER

**Schema** (columns for vendors.xlsx):
```
vendor_id              VARCHAR  VND-{6-digit zero-padded}
vendor_code            VARCHAR  V{5-digit}
vendor_name            VARCHAR  Company name (Faker.company)
vendor_legal_name      VARCHAR  Legal entity name
vendor_type            VARCHAR  SUBCONTRACTOR/SUPPLIER/CONSULTANT/LABOUR_CONTRACTOR/OEM/MANUFACTURER
sector_specialization  VARCHAR  CIVIL/OIL_GAS/POWER/MULTI  (weighted: CIVIL 35%, OIL_GAS 30%, POWER 20%, MULTI 15%)
country                VARCHAR  Weighted: UAE 25%, Saudi Arabia 20%, USA 15%, UK 10%, India 10%, rest 20%
state                  VARCHAR
city                   VARCHAR
postal_code            VARCHAR
address_line1          VARCHAR
registration_number    VARCHAR  REG-{country_code}-{8-digit}
vat_number             VARCHAR  VAT{15-digit}
tax_id                 VARCHAR
bank_name              VARCHAR  (10 major banks: HSBC, Citi, JPMorgan, etc.)
bank_account           VARCHAR  {18-digit IBAN format}
swift_code             VARCHAR
payment_terms_days     INT      Choices: 30, 45, 60, 90 (weighted: 30→20%, 45→30%, 60→35%, 90→15%)
credit_limit_usd       FLOAT    Normal distribution: mean=500000, std=300000, min=10000, max=5000000
performance_rating     FLOAT    Normal dist: mean=3.5, std=0.8, clipped 1.0–5.0
risk_category          VARCHAR  LOW(50%)/MEDIUM(35%)/HIGH(13%)/BLACKLISTED(2%)
prequalification_status VARCHAR APPROVED(70%)/PENDING(15%)/EXPIRED(10%)/REJECTED(5%)
prequalification_expiry_date DATE Random date within next 3 years
iso_certified          BOOLEAN  True for 60% of vendors
hse_rating             INT      1–5, weighted toward 3–4
minority_owned         BOOLEAN  10% True
small_business         BOOLEAN  30% True
contact_name           VARCHAR  Faker.name
contact_email          VARCHAR  Faker.company_email
contact_phone          VARCHAR
annual_turnover_usd    FLOAT    Lognormal: mean=2M, wide spread
years_in_business      INT      5–50 years
created_date           DATE     Random date 2015–2022
last_updated_date      DATE     Random date 2022–2024
```

**Business Rules**:
- BLACKLISTED vendors have performance_rating < 2.0
- APPROVED prequalification vendors have performance_rating > 2.5
- Small businesses have credit_limit_usd < 200,000
- OEM vendors are always iso_certified = True
- OIL_GAS sector vendors have stricter hse_rating (minimum 3)

---

### GENERATOR 02: `02_gen_projects.py` — PROJECT MASTER

**Schema** (projects.xlsx):
```
project_id             VARCHAR  PRJ-{YEAR}-{5-digit}
project_code           VARCHAR  {sector_code}-{country_code}-{4-digit}
project_name           VARCHAR  Descriptive name matching type
project_type           VARCHAR  See 14 types in dim_project above
sector                 VARCHAR  CIVIL_CONSTRUCTION/OIL_GAS/POWER/INFRASTRUCTURE
sub_sector             VARCHAR  Detailed sub-type
client_name            VARCHAR  Government bodies and companies
project_manager        VARCHAR  Faker.name
project_director       VARCHAR  Faker.name
country                VARCHAR  Weighted same as vendors
state, city            VARCHAR
contract_value_usd     FLOAT    Lognormal: median=5M, range=100K–2B
approved_budget_usd    FLOAT    contract_value_usd * random(0.90, 1.05)
project_status         VARCHAR  BIDDING(5%)/AWARDED(5%)/MOBILIZATION(10%)/
                                CONSTRUCTION(50%)/COMMISSIONING(15%)/
                                HANDOVER(10%)/CLOSED(5%)
start_date             DATE     2019–2024
planned_completion_date DATE    start_date + random(180–1800) days
actual_completion_date DATE     NULL if not CLOSED, else planned + offset
phase                  VARCHAR  Based on status
priority               VARCHAR  CRITICAL(10%)/HIGH(30%)/MEDIUM(45%)/LOW(15%)
risk_level             VARCHAR  LOW(30%)/MEDIUM(45%)/HIGH(20%)/EXTREME(5%)
wbs_level_1            VARCHAR  {project_code}-{discipline}
wbs_level_2            VARCHAR  {wbs_level_1}-{sub_discipline}
wbs_level_3            VARCHAR  {wbs_level_2}-{activity}
environmental_clearance BOOLEAN True for 85% of projects
latitude               FLOAT   Realistic coords per country
longitude              FLOAT
created_date           DATE
```

**Business Rules**:
- HIGHWAY and BRIDGE projects are always CIVIL_CONSTRUCTION sector
- REFINERY, PIPELINE, DRILLING, LNG are always OIL_GAS
- POWER_THERMAL, POWER_SOLAR, POWER_WIND, SUBSTATION are always POWER
- Contract values: POWER_PLANT > REFINERY > HIGHWAY > BUILDING > APARTMENT
- CRITICAL priority projects have highest contract values (top 10%)

---

### GENERATOR 03: `03_gen_materials.py` — MATERIAL/ITEM CATALOG

**Schema** (materials.xlsx):
```
material_id            VARCHAR  MAT-{8-digit}
material_code          VARCHAR  {category_code}-{sub_code}-{sequence}
material_name          VARCHAR  Specific material name
material_description   VARCHAR  Full technical description
material_level         VARCHAR  PRODUCT(20%)/ITEM(60%)/COMPONENT(15%)/ASSEMBLY(5%)
parent_material_id     VARCHAR  NULL for PRODUCT; references parent for ITEM/COMPONENT
product_category       VARCHAR  15 categories listed in dim_material above
sub_category           VARCHAR  3–5 sub-categories per category
commodity_code         VARCHAR  UNSPSC: {8-digit hierarchical code}
hsn_code               VARCHAR  {6-digit}
unit_of_measure        VARCHAR  Per category (steel→KG/MT, pipes→RM, concrete→CUM)
standard_unit_price_usd FLOAT   Realistic market prices per category
price_currency         VARCHAR  USD/EUR/GBP/SAR
lead_time_days         INT      Category-specific: steel=90, electrical=45, civil=14
minimum_order_quantity  FLOAT
shelf_life_days        INT      NULL for non-perishable; 30–365 for chemicals
hazardous_material     BOOLEAN  True for chemicals, fuels (25%)
requires_inspection    BOOLEAN  True for critical items (40%)
strategic_item         BOOLEAN  True for long-lead items (15%)
local_content          BOOLEAN  True for 40%
```

**Business Rules**:
- PIPES category includes: Carbon Steel Pipe, SS Pipe, HDPE Pipe, GRE Pipe (O&G specific)
- VALVES category: Gate Valve, Globe Valve, Check Valve, Ball Valve, Control Valve
- STRUCTURAL_STEEL: H-Beams, I-Beams, Angle Sections, Hollow Sections, Plates
- ELECTRICAL: Cables, Transformers, Switchgear, MCC Panels, UPS
- INSTRUMENTS: Flow Meters, Pressure Gauges, Transmitters, Control Systems (high value)
- EQUIPMENT: Cranes, Excavators, Pumps, Compressors, Generators
- unit prices must be realistic: Crane rental=5000/day, Steel=800/MT, Cables=50/RM

---

### GENERATOR 04: `04_gen_employees.py` — EMPLOYEE MASTER

**Schema** (employees.xlsx):
```
employee_id            VARCHAR  EMP-{6-digit}
employee_code          VARCHAR  {dept_code}{5-digit}
employee_name          VARCHAR  Faker.name
department             VARCHAR  PROCUREMENT/ENGINEERING/FINANCE/PROJECTS/HSE/LEGAL/CONTRACTS
designation            VARCHAR  Department-specific titles
grade                  VARCHAR  L1/L2/L3/L4/L5/L6 (L6=Director)
approval_limit_usd     FLOAT    Grade-based: L1=10K, L2=50K, L3=200K, L4=500K, L5=2M, L6=unlimited
cost_center_id         VARCHAR  Reference to a cost center
email                  VARCHAR  Faker.company_email
phone                  VARCHAR
hire_date              DATE     2010–2023
nationality            VARCHAR
active                 BOOLEAN  90% True
```

---

### GENERATOR 05: `05_gen_purchase_orders.py` — PO HEADERS

**Schema** (purchase_orders.xlsx):
```
po_id                  VARCHAR  PO-{YEAR}-{7-digit}
po_number              VARCHAR  {project_code}/PO/{sequence}
vendor_id              VARCHAR  FK → vendors.vendor_id (valid reference)
project_id             VARCHAR  FK → projects.project_id
contract_id            VARCHAR  FK → contracts.contract_id (nullable for spot POs)
buyer_employee_id      VARCHAR  FK → employees.employee_id (PROCUREMENT dept)
approver_employee_id   VARCHAR  FK → employees.employee_id (higher grade)
po_type                VARCHAR  STANDARD(50%)/BLANKET(20%)/EMERGENCY(10%)/FRAMEWORK(20%)
po_status              VARCHAR  DRAFT(5%)/APPROVED(15%)/ISSUED(10%)/PARTIALLY_RECEIVED(30%)/
                                FULLY_RECEIVED(30%)/CLOSED(8%)/CANCELLED(2%)
priority_flag          VARCHAR  NORMAL(70%)/URGENT(20%)/CRITICAL(10%)
order_date             DATE     2020–2024
required_date          DATE     order_date + lead_time_days
promised_date          DATE     required_date ± random offset
delivery_date          DATE     NULL if not received; else promised ± offset
incoterms              VARCHAR  EXW/FCA/CPT/CIP/DAP/DDP/FOB/CFR/CIF
currency_code          VARCHAR  USD(60%)/EUR(20%)/GBP(10%)/SAR(10%)
payment_terms          VARCHAR  Based on vendor payment_terms_days
total_po_value_usd     FLOAT    Sum of line items (calculated)
total_received_value   FLOAT
notes                  TEXT     Faker.sentence
created_by             VARCHAR  buyer_employee_id
created_date           TIMESTAMP
last_modified_date     TIMESTAMP
```

**Business Rules**:
- EMERGENCY POs must have priority=CRITICAL and shorter lead times
- BLANKET POs have higher total values (framework agreements)
- vendor_id must be from APPROVED prequalification vendors only (filter vendors list)
- Buyers must have approval_limit_usd >= po_line_item total for that PO

---

### GENERATOR 06: `06_gen_po_line_items.py` — PO LINE ITEMS

**Schema** (po_line_items.xlsx):
```
line_id                VARCHAR  {po_id}-L{3-digit}
po_id                  VARCHAR  FK → purchase_orders.po_id
po_number              VARCHAR
line_number            INT      Sequential per PO (1, 2, 3...)
material_id            VARCHAR  FK → materials.material_id
material_code          VARCHAR
material_description   VARCHAR
unit_of_measure        VARCHAR  From materials
quantity_ordered       FLOAT    Realistic quantities per UOM
unit_price_usd         FLOAT    ± 20% of material standard_unit_price
line_amount_usd        FLOAT    quantity_ordered * unit_price_usd
tax_pct                FLOAT    0/5/10/15/20 based on material category
tax_amount_usd         FLOAT    line_amount * tax_pct/100
discount_pct           FLOAT    0–10%
discount_amount_usd    FLOAT
total_line_amount_usd  FLOAT    line_amount + tax - discount
quantity_received      FLOAT    0 to quantity_ordered (based on PO status)
received_value_usd     FLOAT    quantity_received * unit_price_usd
outstanding_qty        FLOAT    quantity_ordered - quantity_received
outstanding_value_usd  FLOAT
delivery_note_number   VARCHAR  DN-{8-digit} (NULL if not received)
required_date          DATE
delivery_date          DATE     NULL or actual delivery
line_status            VARCHAR  OPEN/PARTIALLY_RECEIVED/FULLY_RECEIVED/CANCELLED
inspection_required    BOOLEAN  From material.requires_inspection
inspection_passed      BOOLEAN  True for 92% of received items
rejection_quantity     FLOAT    Remainder if inspection failed
rejection_reason       VARCHAR  QUALITY/WRONG_SPEC/DAMAGED/SHORT_SUPPLY
```

**Business Rules**:
- Each PO has between 1 and 20 line items (weighted: 1–5 most common)
- Total line items across all 100K POs = exactly 100,000 rows in this file
  → Adjust so average lines per PO makes total = 100K
- Steel and pipe quantities: in MT or RM (large quantities)
- Instrument quantities: in NOS (small quantities, high unit price)
- If PO status = CANCELLED, quantity_received = 0

---

### GENERATOR 07: `07_gen_contracts.py` — CONTRACT MASTER

**Schema** (contracts.xlsx):
```
contract_id            VARCHAR  CTR-{YEAR}-{6-digit}
contract_number        VARCHAR  {project_code}/CTR/{sequence}
contract_title         VARCHAR  Descriptive title
vendor_id              VARCHAR  FK → vendors
project_id             VARCHAR  FK → projects
contract_manager_id    VARCHAR  FK → employees (CONTRACTS dept)
contract_type          VARCHAR  LUMP_SUM(40%)/REMEASURE(30%)/COST_PLUS(15%)/TURNKEY(10%)/EPC(5%)
contract_category      VARCHAR  MAIN_CONTRACT(30%)/SUBCONTRACT(40%)/SUPPLY(20%)/SERVICE(10%)
scope_of_work          VARCHAR  Faker.sentence(nb_words=15)
contract_currency      VARCHAR
original_contract_value FLOAT   Lognormal: median=1M, range=50K–500M
revised_contract_value  FLOAT   original * random(0.8, 1.3) — accounts for change orders
total_paid             FLOAT    0 to revised_contract_value based on status
award_date             DATE
start_date             DATE     award_date + 30–90 days
planned_completion     DATE
actual_completion      DATE     NULL if active
contract_status        VARCHAR  TENDERING(5%)/AWARDED(10%)/ACTIVE(50%)/
                                COMPLETED(25%)/DISPUTED(5%)/TERMINATED(5%)
mobilization_advance_pct FLOAT  5–20%
retention_pct          FLOAT    5–10%
retention_held         FLOAT    Calculated
retention_released     FLOAT    Partial release on completion milestones
performance_bond_pct   FLOAT    5–15%
defect_liability_months INT     12/18/24/36
payment_terms          VARCHAR  Monthly progress/Milestone/30-60-90 days
governing_law          VARCHAR  Country of project
dispute_resolution     VARCHAR  ICC Arbitration/LCIA/SIAC/Local Courts
parent_contract_id     VARCHAR  NULL for main contracts; ref for subcontracts
number_of_variations   INT      0–20
variation_amount       FLOAT    Change order total
completion_pct         FLOAT    0–100
created_date, last_modified_date  TIMESTAMP
```

---

### GENERATOR 08: `08_gen_contract_items.py` — CONTRACT LINE ITEMS (BOQ)

**Schema** (contract_items.xlsx):
```
item_id                VARCHAR  {contract_id}-BOQ-{4-digit}
contract_id            VARCHAR  FK → contracts
boq_item_number        VARCHAR  Section.subsection.item format (e.g., 2.3.1)
item_description       VARCHAR  Technical scope description
material_id            VARCHAR  FK → materials (the item/activity type)
unit_of_measure        VARCHAR
contracted_quantity    FLOAT
contracted_unit_rate   FLOAT
contracted_amount      FLOAT    contracted_quantity * contracted_unit_rate
revised_quantity       FLOAT    ± variation
revised_amount         FLOAT
paid_quantity          FLOAT    Quantity certified for payment to date
paid_amount            FLOAT    Earned value paid
retention_held_pct     FLOAT    Per contract terms
milestone_flag         BOOLEAN  True for lump sum milestones (15%)
milestone_description  VARCHAR  NULL unless milestone_flag
completion_pct         FLOAT
```

---

### GENERATOR 09: `09_gen_invoices.py` — VENDOR INVOICES

**Schema** (invoices.xlsx):
```
invoice_id             VARCHAR  INV-{YEAR}-{7-digit}
invoice_number         VARCHAR  Vendor's own invoice number (Faker)
vendor_id              VARCHAR  FK → vendors
po_id                  VARCHAR  FK → purchase_orders (nullable for contract invoices)
contract_id            VARCHAR  FK → contracts (nullable for PO invoices)
project_id             VARCHAR  FK → projects
invoice_type           VARCHAR  MATERIAL(40%)/SERVICE(20%)/PROGRESS(30%)/ADVANCE(10%)
invoice_date           DATE     2020–2024
due_date               DATE     invoice_date + payment_terms
receipt_date           DATE     invoice_date + 1–5 days
approval_date          DATE     receipt_date + 3–15 days
payment_date           DATE     NULL if unpaid; else due_date ± offset
approver_id            VARCHAR  FK → employees
invoice_currency       VARCHAR
gross_invoice_amount   FLOAT
tax_amount             FLOAT
retention_deducted     FLOAT    For contract invoices
advance_deducted       FLOAT    Recovery of mobilization advance
other_deductions       FLOAT
net_payable_amount     FLOAT    gross + tax - retention - advance - other
approved_amount        FLOAT    ≤ net_payable (partial approvals exist)
disputed_amount        FLOAT    Gross - approved (for disputed invoices)
payment_method         VARCHAR  BANK_TRANSFER/CHEQUE/LC/SWIFT
invoice_status         VARCHAR  RECEIVED(5%)/UNDER_REVIEW(10%)/APPROVED(20%)/
                                DISPUTED(5%)/PAID(55%)/PARTIALLY_PAID(5%)
days_outstanding       INT      TODAY - invoice_date if unpaid
early_payment_discount FLOAT    2% if paid within 10 days
payment_reference      VARCHAR  Bank reference number
notes                  TEXT
```

---

### GENERATOR 10: `10_gen_goods_receipts.py` — GRN RECORDS

**Schema** (goods_receipts.xlsx):
```
grn_id                 VARCHAR  GRN-{YEAR}-{7-digit}
grn_number             VARCHAR  {project_code}/GRN/{sequence}
po_id                  VARCHAR  FK → purchase_orders
po_line_id             VARCHAR  FK → po_line_items
vendor_id              VARCHAR  FK → vendors
material_id            VARCHAR  FK → materials
project_id             VARCHAR  FK → projects
site_id                VARCHAR  FK → geography (delivery site)
receipt_date           DATE
inspector_id           VARCHAR  FK → employees
quantity_delivered     FLOAT    Per delivery note
quantity_accepted      FLOAT    ≤ quantity_delivered
quantity_rejected      FLOAT    quantity_delivered - quantity_accepted
rejection_reason       VARCHAR  QUALITY/WRONG_SPEC/DAMAGED/SHORT_SUPPLY/EXCESS (if any)
unit_price_at_receipt  FLOAT    PO unit price at time of receipt
accepted_value_usd     FLOAT    quantity_accepted * unit_price_at_receipt
rejected_value_usd     FLOAT
delivery_note_number   VARCHAR
vehicle_number         VARCHAR  Faker plate number
driver_name            VARCHAR
inspection_certificate VARCHAR  CERT-{8-digit} (if requires_inspection)
storage_location       VARCHAR  {site_code}-WAREHOUSE-{section}
grn_status             VARCHAR  DRAFT/ACCEPTED/PARTIALLY_ACCEPTED/REJECTED/POSTED
created_timestamp      TIMESTAMP
```

---

### GENERATOR 11: `11_gen_project_budgets.py` — PROJECT BUDGETS

**Schema** (project_budgets.xlsx):
```
budget_id              VARCHAR  BDG-{project_id}-{wbs_code}-{version}
project_id             VARCHAR  FK → projects
wbs_code               VARCHAR  Hierarchical WBS code
wbs_description        VARCHAR  Description of work package
cost_type              VARCHAR  LABOUR/MATERIAL/EQUIPMENT/SUBCONTRACT/OVERHEAD/CONTINGENCY
discipline             VARCHAR  From dim_cost_center.discipline
budget_version         VARCHAR  ORIGINAL(40%)/REVISION_1(30%)/REVISION_2(20%)/REVISION_3(10%)
original_budget_usd    FLOAT    The baseline cost estimate
approved_budget_usd    FLOAT    original ± change orders
contingency_usd        FLOAT    5–15% of approved budget
period_month           DATE     First day of month (monthly budget)
monthly_budget_usd     FLOAT    Phased/spread version of annual budget
cumulative_budget_usd  FLOAT    Running cumulative budget
approval_date          DATE
approved_by            VARCHAR  FK → employees
budget_status          VARCHAR  DRAFT/PENDING_APPROVAL/APPROVED/SUPERSEDED
change_order_reference VARCHAR  CO-{number} if revision
notes                  TEXT
```

---

### GENERATOR 12: `12_gen_project_actuals.py` — ACTUAL COST TRANSACTIONS

**Schema** (project_actuals.xlsx):
```
transaction_id         VARCHAR  TXN-{YEAR}-{8-digit}
project_id             VARCHAR  FK → projects
wbs_code               VARCHAR  FK → project_budgets.wbs_code
cost_type              VARCHAR  LABOUR/MATERIAL/EQUIPMENT/SUBCONTRACT/OVERHEAD
discipline             VARCHAR
vendor_id              VARCHAR  FK → vendors (supplier/subcontractor)
po_id                  VARCHAR  FK → purchase_orders (nullable)
contract_id            VARCHAR  FK → contracts (nullable)
invoice_id             VARCHAR  FK → invoices (nullable)
transaction_date       DATE     2020–2024
period_month           DATE     First of month
actual_quantity        FLOAT
unit_of_measure        VARCHAR
unit_cost_usd          FLOAT
actual_amount_usd      FLOAT    actual_quantity * unit_cost_usd
currency_code          VARCHAR
exchange_rate          FLOAT
committed_amount_usd   FLOAT    Committed but not yet incurred
forecast_amount_usd    FLOAT    Budget remaining estimate
budget_amount_usd      FLOAT    From project_budgets for this WBS/period
variance_amount_usd    FLOAT    actual - budget
variance_pct           FLOAT    variance / budget * 100
cost_status            VARCHAR  POSTED/ACCRUED/COMMITTED/FORECAST
change_order_flag      BOOLEAN  True if this cost is outside original scope
change_order_ref       VARCHAR  CO-{number}
cost_code              VARCHAR  Company internal code
gl_account             VARCHAR  General Ledger account number
posting_date           DATE
posted_by              VARCHAR  FK → employees
notes                  TEXT
```

---

### GENERATOR 13: `13_gen_sales_orders.py` — SALES ORDERS

**Schema** (sales_orders.xlsx):
```
sales_order_id         VARCHAR  SO-{YEAR}-{7-digit}
sales_order_number     VARCHAR  {client_code}/SO/{sequence}
project_id             VARCHAR  FK → projects (project being billed)
client_id              VARCHAR  Client/customer ID
client_name            VARCHAR  Government body or private client
salesperson_id         VARCHAR  FK → employees
billing_milestone      VARCHAR  Description of billing milestone
material_id            VARCHAR  FK → materials (what's being sold/delivered)
product_description    VARCHAR
-- PRODUCT LEVEL metrics:
product_category       VARCHAR  Top-level product grouping
product_quantity       FLOAT    Quantity at product level
product_unit_price_usd FLOAT    Revenue per product unit
product_revenue_usd    FLOAT    product_quantity * product_unit_price
-- ITEM LEVEL metrics (more granular):
item_quantity          FLOAT    Breakdown quantity at item level
item_unit_price_usd    FLOAT
item_revenue_usd       FLOAT    item_quantity * item_unit_price
-- Cost metrics:
cogs_usd               FLOAT    Cost of Goods Sold (60–85% of revenue)
gross_margin_usd       FLOAT    Revenue - COGS
gross_margin_pct       FLOAT
discount_given_usd     FLOAT
net_revenue_usd        FLOAT    Revenue - Discount
tax_collected_usd      FLOAT
order_date             DATE
billing_date           DATE
delivery_date          DATE
currency_code          VARCHAR
exchange_rate          FLOAT
payment_terms          VARCHAR
payment_status         VARCHAR  UNBILLED/BILLED/PARTIALLY_PAID/PAID/OVERDUE
sector                 VARCHAR  From project sector
region                 VARCHAR  From project geography
```

---

### GENERATOR 14: `14_gen_inventory.py` — INVENTORY MOVEMENTS

**Schema** (inventory.xlsx):
```
movement_id            VARCHAR  INV-MOV-{8-digit}
movement_type          VARCHAR  RECEIPT/ISSUE/RETURN/TRANSFER/ADJUSTMENT/WRITEOFF
material_id            VARCHAR  FK → materials
project_id             VARCHAR  FK → projects
from_location          VARCHAR  Warehouse/site location
to_location            VARCHAR
grn_id                 VARCHAR  FK → goods_receipts (for RECEIPT movements)
movement_date          DATE
quantity               FLOAT    Positive for RECEIPT/RETURN, negative for ISSUE/WRITEOFF
unit_of_measure        VARCHAR
unit_cost_usd          FLOAT    Weighted average cost
total_cost_usd         FLOAT
balance_quantity       FLOAT    Running stock balance at location
balance_value_usd      FLOAT
batch_number           VARCHAR  For trackable items
expiry_date            DATE     For shelf-life items
moved_by               VARCHAR  FK → employees
authorized_by          VARCHAR  FK → employees
reference_document     VARCHAR  PO/WO/TR reference
notes                  TEXT
```

---

### GENERATOR 15: `15_gen_vendor_performance.py` — VENDOR PERFORMANCE RECORDS

**Schema** (vendor_perf.xlsx):
```
performance_id         VARCHAR  PERF-{vendor_id}-{YYYYMM}
vendor_id              VARCHAR  FK → vendors
project_id             VARCHAR  FK → projects
evaluation_period      VARCHAR  YYYY-MM
evaluator_id           VARCHAR  FK → employees
-- Delivery metrics:
on_time_delivery_pct   FLOAT    0–100
delivery_quantity_accuracy_pct FLOAT
-- Quality metrics:
acceptance_rate_pct    FLOAT    100 - rejection_rate
defect_rate_pct        FLOAT
ncr_count              INT      Non-conformance reports
-- Commercial metrics:
invoice_accuracy_pct   FLOAT
price_competitiveness  FLOAT    Vs market benchmark (1=equal, <1=cheaper, >1=expensive)
-- HSE metrics:
hse_incidents          INT      0–5
hse_near_misses        INT      0–10
hse_compliance_pct     FLOAT    0–100
-- Responsiveness:
query_response_days    FLOAT    Average days to respond
-- Scores:
delivery_score         FLOAT    Weighted 0–100
quality_score          FLOAT    Weighted 0–100
commercial_score       FLOAT    Weighted 0–100
hse_score              FLOAT    Weighted 0–100
overall_score          FLOAT    Weighted average (delivery 30%, quality 30%, commercial 20%, hse 20%)
recommendation         VARCHAR  PREFERRED/APPROVED/CONDITIONAL/DELIST
comments               TEXT
```

---

## BRONZE LAYER — DLT PIPELINE (PIPELINE 1)

### File: `databricks/bronze/01_bronze_pipeline.py`

This is the complete DLT pipeline for all bronze ingestion. Generate the FULL file with ALL tables.

```python
"""
Bronze DLT Pipeline — Raw Ingestion
Pipeline Name: procurement_bronze_pipeline
Catalog: procurement_dev
Schema: bronze
Source: ADLS volumes /Volumes/procurement_dev/bronze/raw_files/

Tables created:
  bronze.raw_vendors
  bronze.raw_projects
  bronze.raw_materials
  bronze.raw_employees
  bronze.raw_purchase_orders
  bronze.raw_po_line_items
  bronze.raw_contracts
  bronze.raw_contract_items
  bronze.raw_invoices
  bronze.raw_goods_receipts
  bronze.raw_project_budgets
  bronze.raw_project_actuals
  bronze.raw_sales_orders
  bronze.raw_inventory
  bronze.raw_vendor_performance

Metadata columns added to every table:
  _ingestion_timestamp  TIMESTAMP  current_timestamp()
  _source_file          STRING     input_file_name()
  _batch_id             STRING     UUID
  _processing_date      DATE       current_date()
  _record_hash          STRING     sha2(concat_ws('|', all_columns), 256)
"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Configuration — set these as pipeline parameters
CATALOG = spark.conf.get("pipeline.catalog", "procurement_dev")
VOLUME_PATH = f"/Volumes/{CATALOG}/bronze/raw_files"
BATCH_ID = spark.conf.get("pipeline.batch_id", F.expr("uuid()"))

# ─── HELPER FUNCTION ────────────────────────────────────────────────
def add_bronze_metadata(df, source_name):
    """Add standard bronze metadata columns to any dataframe."""
    return df \
        .withColumn("_ingestion_timestamp", F.current_timestamp()) \
        .withColumn("_source_file", F.input_file_name()) \
        .withColumn("_batch_id", F.lit(BATCH_ID)) \
        .withColumn("_processing_date", F.current_date()) \
        .withColumn("_record_hash", F.sha2(
            F.concat_ws("|", *[F.col(c).cast("string") 
                               for c in df.columns]), 256))

# ─── BRONZE TABLE: raw_vendors ───────────────────────────────────────
@dlt.table(
    name="raw_vendors",
    comment="Raw vendor master data ingested from vendors.xlsx",
    table_properties={"quality": "bronze", "domain": "procurement"},
    path=f"{VOLUME_PATH}/bronze/raw_vendors"
)
def raw_vendors():
    df = (spark.readStream
          .format("cloudFiles")
          .option("cloudFiles.format", "com.crealytics.spark.excel")
          .option("cloudFiles.schemaLocation", 
                  f"{VOLUME_PATH}/_schema_checkpoints/raw_vendors")
          .option("header", "true")
          .option("inferSchema", "true")
          .option("dataAddress", "'Sheet1'!A1")
          .load(f"{VOLUME_PATH}/vendors/*.xlsx"))
    return add_bronze_metadata(df, "vendors")

# ─── BRONZE TABLE: raw_projects ─────────────────────────────────────
@dlt.table(
    name="raw_projects",
    comment="Raw project master data",
    table_properties={"quality": "bronze", "domain": "projects"}
)
def raw_projects():
    df = (spark.readStream.format("cloudFiles")
          .option("cloudFiles.format", "com.crealytics.spark.excel")
          .option("cloudFiles.schemaLocation",
                  f"{VOLUME_PATH}/_schema_checkpoints/raw_projects")
          .option("header", "true").option("inferSchema", "true")
          .load(f"{VOLUME_PATH}/projects/*.xlsx"))
    return add_bronze_metadata(df, "projects")

# [COPILOT: REPEAT THIS PATTERN FOR ALL 15 TABLES]
# Tables: raw_materials, raw_employees, raw_purchase_orders,
#         raw_po_line_items, raw_contracts, raw_contract_items,
#         raw_invoices, raw_goods_receipts, raw_project_budgets,
#         raw_project_actuals, raw_sales_orders, raw_inventory,
#         raw_vendor_performance
```

**COPILOT INSTRUCTION**: Complete this file with all 15 `@dlt.table` functions following the exact same pattern. Every table reads from its corresponding xlsx file path under `VOLUME_PATH/{table_name}/`.

---

## SILVER LAYER — DLT PIPELINE (PIPELINE 2)

### File: `databricks/silver/02_silver_pipeline.py`

```python
"""
Silver DLT Pipeline — Cleansing, Conforming, Quality Enforcement
Pipeline Name: procurement_silver_pipeline
Catalog: procurement_dev
Schema: silver
Reads from: procurement_dev.bronze.*
Writes to:  procurement_dev.silver.*

DQ Strategy:
  - @dlt.expect_or_drop: Critical columns (PKs, FKs) — drop row if violated
  - @dlt.expect_or_quarantine: Business rules — quarantine for review
  - @dlt.expect: Warnings — log but do not drop
  - All quarantined records go to silver.dq_quarantine table
  - DQ metrics logged to silver.dq_metrics table

Transformations per table:
  - Trim all string columns
  - Standardize case (vendor_name → Title Case; codes → UPPER)
  - Parse dates to DATE type with error handling
  - Cast amounts to DECIMAL(18,2)
  - Null imputation per business rules
  - Deduplication on natural keys
  - Add silver metadata: _silver_timestamp, _dq_status, _dq_score
"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import *

CATALOG = spark.conf.get("pipeline.catalog", "procurement_dev")

# ─── SILVER: clean_vendors ──────────────────────────────────────────
@dlt.table(
    name="clean_vendors",
    comment="Cleansed and conformed vendor master — deduplicated, normalized",
    table_properties={"quality": "silver", "domain": "procurement"}
)
@dlt.expect_or_drop("valid_vendor_id", "vendor_id IS NOT NULL AND vendor_id != ''")
@dlt.expect_or_drop("valid_vendor_name", "vendor_name IS NOT NULL AND LENGTH(vendor_name) > 1")
@dlt.expect_or_quarantine("valid_email", "contact_email RLIKE '^[A-Za-z0-9+_.-]+@[A-Za-z0-9.-]+$'")
@dlt.expect_or_quarantine("valid_performance_rating", 
                           "performance_rating BETWEEN 1.0 AND 5.0")
@dlt.expect_or_quarantine("valid_payment_terms", 
                           "payment_terms_days IN (30, 45, 60, 90)")
@dlt.expect("non_negative_credit_limit", "credit_limit_usd >= 0")
def clean_vendors():
    return (
        dlt.read_stream("raw_vendors")
        .filter(F.col("_rescued_data").isNull())  # Drop unrecoverable rows
        # ── Trim strings ──────────────────────────────────────────────
        .withColumn("vendor_id",     F.trim(F.col("vendor_id")))
        .withColumn("vendor_name",   F.initcap(F.trim(F.col("vendor_name"))))
        .withColumn("vendor_code",   F.upper(F.trim(F.col("vendor_code"))))
        .withColumn("vendor_type",   F.upper(F.trim(F.col("vendor_type"))))
        .withColumn("risk_category", F.upper(F.trim(F.col("risk_category"))))
        .withColumn("country",       F.upper(F.trim(F.col("country"))))
        # ── Type casting ──────────────────────────────────────────────
        .withColumn("credit_limit_usd",  F.col("credit_limit_usd").cast("decimal(18,2)"))
        .withColumn("performance_rating",F.col("performance_rating").cast("decimal(3,1)"))
        .withColumn("payment_terms_days",F.col("payment_terms_days").cast("integer"))
        .withColumn("annual_turnover_usd",F.col("annual_turnover_usd").cast("decimal(18,2)"))
        # ── Date parsing ──────────────────────────────────────────────
        .withColumn("created_date",
            F.coalesce(
                F.to_date(F.col("created_date"), "yyyy-MM-dd"),
                F.to_date(F.col("created_date"), "dd/MM/yyyy"),
                F.to_date(F.col("created_date"), "MM/dd/yyyy")
            ))
        .withColumn("prequalification_expiry_date",
            F.coalesce(
                F.to_date(F.col("prequalification_expiry_date"), "yyyy-MM-dd"),
                F.to_date(F.col("prequalification_expiry_date"), "dd/MM/yyyy")
            ))
        # ── Standardize enum values ───────────────────────────────────
        .withColumn("prequalification_status",
            F.when(F.col("prequalification_status").isin(
                ["APPROVED","PENDING","EXPIRED","REJECTED"]),
                F.col("prequalification_status"))
            .otherwise("PENDING"))
        .withColumn("sector_specialization",
            F.when(F.col("sector_specialization").isin(
                ["CIVIL","OIL_GAS","POWER","MULTI"]),
                F.col("sector_specialization"))
            .otherwise("MULTI"))
        # ── Deduplication: keep latest record per vendor_id ───────────
        .withColumn("_row_num", F.row_number().over(
            Window.partitionBy("vendor_id")
            .orderBy(F.col("last_updated_date").desc())))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
        # ── Silver metadata ───────────────────────────────────────────
        .withColumn("_silver_timestamp", F.current_timestamp())
        .withColumn("_dq_status", F.lit("PASSED"))
        .withColumn("_dq_score", F.lit(1.0))
    )

# [COPILOT: Generate clean_projects, clean_materials, clean_employees,
#  clean_purchase_orders, clean_po_line_items, clean_contracts,
#  clean_contract_items, clean_invoices, clean_goods_receipts,
#  clean_project_budgets, clean_project_actuals, clean_sales_orders,
#  clean_inventory, clean_vendor_performance]
#
# Each table must have:
#  1. Minimum 3 @dlt.expect_or_drop rules for PK/FK integrity
#  2. Minimum 4 @dlt.expect_or_quarantine rules for business logic
#  3. String trimming and case normalization for all VARCHAR columns
#  4. Safe date parsing with multiple format fallbacks
#  5. Amount casting to DECIMAL(18,2)
#  6. Deduplication on natural key
#  7. Silver metadata columns (_silver_timestamp, _dq_status, _dq_score)
```

### Silver-specific business rules per table:

**clean_purchase_orders**:
- `po_total_usd > 0` → expect_or_drop
- `order_date <= required_date` → expect_or_quarantine
- `delivery_date IS NULL OR delivery_date >= order_date` → expect_or_quarantine
- Compute `delivery_days_variance = delivery_date - promised_date` (negative = early, positive = late)
- Compute `days_open = CASE WHEN po_status = 'CLOSED' THEN datediff(delivery_date, order_date) ELSE datediff(current_date, order_date) END`

**clean_contracts**:
- `original_contract_value > 0` → expect_or_drop
- `start_date >= award_date` → expect_or_quarantine
- `planned_completion > start_date` → expect_or_quarantine
- Compute `contract_duration_days = datediff(planned_completion, start_date)`
- Compute `overrun_days = CASE WHEN actual_completion IS NOT NULL THEN datediff(actual_completion, planned_completion) ELSE datediff(current_date, planned_completion) END` (only for active/overdue)
- Compute `cost_growth_pct = (revised_contract_value - original_contract_value) / original_contract_value * 100`

**clean_invoices**:
- `net_payable_amount >= 0` → expect_or_drop
- `invoice_date <= due_date` → expect_or_quarantine
- `approved_amount <= gross_invoice_amount` → expect_or_quarantine
- Compute `aging_bucket = CASE WHEN payment_date IS NULL AND datediff(current_date, due_date) > 90 THEN '90+ DAYS' WHEN ... END`

**clean_project_actuals**:
- `actual_amount_usd IS NOT NULL AND actual_amount_usd >= 0` → expect_or_drop
- `transaction_date <= current_date` → expect_or_quarantine
- Compute `cpi = earned_value / actual_amount_usd` (Cost Performance Index, only if earned_value available)
- Flag `over_budget = actual_amount_usd > budget_amount_usd`

---

## GOLD LAYER — DLT PIPELINE (PIPELINE 3)

### File: `databricks/gold/03_gold_pipeline.py`

This pipeline builds all dimensions (with SCD2 via Delta MERGE) and all fact tables.

#### SCD2 Dimension Build Pattern (use for ALL 10 dimensions):

```python
# ─── DIM_VENDOR (SCD Type 2) ────────────────────────────────────────
@dlt.table(
    name="dim_vendor",
    comment="SCD Type 2 Vendor Dimension — tracks all historical changes",
    table_properties={"quality": "gold", "domain": "procurement",
                      "delta.enableChangeDataFeed": "true"}
)
def dim_vendor():
    """
    SCD2 logic:
    1. Hash all tracked attributes
    2. Compare hash with current record in dim_vendor
    3. If changed: close old record (eff_end_date = today - 1, is_current = False)
               and insert new record (eff_start_date = today, is_current = True)
    4. If new: insert with eff_start_date = vendor.created_date, is_current = True
    5. Surrogate key = monotonically_increasing_id() on new inserts
    """
    from pyspark.sql.window import Window
    
    source = dlt.read("clean_vendors")
    
    # Build tracked attribute hash (all columns that should trigger SCD2)
    tracked_cols = ["vendor_name", "vendor_legal_name", "vendor_type",
                    "sector_specialization", "country", "state", "city",
                    "payment_terms_days", "credit_limit_usd", "performance_rating",
                    "risk_category", "prequalification_status", "iso_certified",
                    "hse_rating", "contact_name", "contact_email", "contact_phone"]
    
    return (
        source
        .withColumn("record_hash",
            F.sha2(F.concat_ws("|", *[F.col(c).cast("string") 
                                       for c in tracked_cols]), 256))
        .withColumn("vendor_key",
            F.monotonically_increasing_id())
        .withColumn("eff_start_date",
            F.coalesce(F.col("created_date"), F.current_date()))
        .withColumn("eff_end_date",
            F.lit("9999-12-31").cast("date"))
        .withColumn("is_current", F.lit(True))
    )
```

#### Fact Table Build Pattern:

```python
# ─── FACT_PURCHASE_ORDERS ────────────────────────────────────────────
@dlt.table(
    name="fact_purchase_orders",
    comment="""
    Fact table: Purchase Orders at line-item grain.
    Joins PO lines → PO headers → dimension tables.
    One row = one PO line item.
    Measures: amount, quantity, delivery performance, price variance.
    """,
    table_properties={"quality": "gold", "domain": "procurement",
                      "delta.enableChangeDataFeed": "true"}
)
def fact_purchase_orders():
    lines   = dlt.read("clean_po_line_items")
    headers = dlt.read("clean_purchase_orders")
    vendors = dlt.read("dim_vendor").filter(F.col("is_current") == True)
    mats    = dlt.read("dim_material").filter(F.col("is_current") == True)
    projs   = dlt.read("dim_project").filter(F.col("is_current") == True)
    dates   = dlt.read("dim_date")
    employees = dlt.read("dim_employee").filter(F.col("is_current") == True)
    currencies = dlt.read("dim_currency")
    geogs   = dlt.read("dim_geography")
    sectors = dlt.read("dim_sector")
    
    return (
        lines.alias("l")
        # ── Join to PO header ─────────────────────────────────────────
        .join(headers.alias("h"), "po_id", "left")
        # ── Lookup dimension keys ─────────────────────────────────────
        .join(vendors.select("vendor_key","vendor_id").alias("v"),
              F.col("h.vendor_id") == F.col("v.vendor_id"), "left")
        .join(mats.select("material_key","material_id").alias("m"),
              F.col("l.material_id") == F.col("m.material_id"), "left")
        .join(projs.select("project_key","project_id").alias("p"),
              F.col("h.project_id") == F.col("p.project_id"), "left")
        .join(dates.select("date_key","full_date").alias("od"),
              F.col("h.order_date") == F.col("od.full_date"), "left")
        .join(dates.select("date_key","full_date").alias("rd"),
              F.col("h.required_date") == F.col("rd.full_date"), "left")
        .join(dates.select("date_key","full_date").alias("dd"),
              F.col("h.delivery_date") == F.col("dd.full_date"), "left")
        .join(employees.select("employee_key","employee_id").alias("buyer"),
              F.col("h.buyer_employee_id") == F.col("buyer.employee_id"), "left")
        .join(currencies.select("currency_key","currency_code").alias("cur"),
              F.col("h.currency_code") == F.col("cur.currency_code"), "left")
        .join(sectors.select("sector_key","sector_code").alias("sec"),
              F.col("h.sector") == F.col("sec.sector_code"), "left")
        # ── Select and name all fact columns ──────────────────────────
        .select(
            # Degenerate dimensions (natural keys kept in fact)
            F.col("l.po_id"),
            F.col("h.po_number"),
            F.col("l.line_id"),
            F.col("l.line_number"),
            # Foreign keys to dimensions
            F.col("v.vendor_key"),
            F.col("m.material_key"),
            F.col("p.project_key"),
            F.col("od.date_key").alias("order_date_key"),
            F.col("rd.date_key").alias("required_date_key"),
            F.col("dd.date_key").alias("delivery_date_key"),
            F.col("buyer.employee_key").alias("buyer_key"),
            F.col("cur.currency_key"),
            F.col("sec.sector_key"),
            # Degenerate dim flags
            F.col("h.po_type"),
            F.col("h.po_status"),
            F.col("h.priority_flag"),
            F.col("l.line_status"),
            F.col("h.incoterms"),
            F.col("l.inspection_required"),
            F.col("l.inspection_passed"),
            F.col("l.rejection_reason"),
            # Measures
            F.col("l.quantity_ordered").cast("decimal(18,4)"),
            F.col("l.quantity_received").cast("decimal(18,4)"),
            F.col("l.unit_price_usd").cast("decimal(18,2)"),
            F.col("l.line_amount_usd").cast("decimal(18,2)"),
            F.col("l.tax_amount_usd").cast("decimal(18,2)"),
            F.col("l.discount_amount_usd").cast("decimal(18,2)"),
            F.col("l.total_line_amount_usd").cast("decimal(18,2)"),
            F.col("l.received_value_usd").cast("decimal(18,2)"),
            F.col("l.outstanding_value_usd").cast("decimal(18,2)"),
            F.col("l.rejection_quantity").cast("decimal(18,4)"),
            # Calculated measures
            F.datediff(F.col("h.delivery_date"), F.col("h.order_date"))
             .alias("days_to_delivery"),
            F.datediff(F.col("h.delivery_date"), F.col("h.promised_date"))
             .alias("delivery_variance_days"),  # negative=early, positive=late
            (F.col("l.quantity_received") / F.col("l.quantity_ordered") * 100)
             .cast("decimal(5,2)").alias("fulfillment_pct"),
            (F.col("l.rejection_quantity") / F.col("l.quantity_delivered") * 100)
             .cast("decimal(5,2)").alias("rejection_rate_pct"),
            # Exchange rate conversion to USD
            (F.col("l.total_line_amount_usd") / F.col("h.exchange_rate"))
             .cast("decimal(18,2)").alias("total_line_amount_base_usd"),
            # Audit
            F.current_timestamp().alias("_gold_timestamp"),
            F.current_date().alias("_processing_date")
        )
    )

# [COPILOT: Generate fact_contracts, fact_invoices, fact_project_costs,
#  fact_goods_receipts, fact_sales using the same join/select pattern]
```

---

## SEMANTIC LAYER — 15 DATA CUBES

### All 15 cubes are Databricks SQL Materialized Views in `procurement_dev.semantic` schema.
### Each refreshes automatically when pipeline triggers are complete.
### All cubes support **Power BI DirectLake** mode.

---

### CUBE 1: `04_cube_procurement_spend.sql` — Total Procurement Spend

```sql
-- MATERIALIZED VIEW: cube_procurement_spend
-- Refresh: SCHEDULE CRON '0 6 * * *'  (daily at 6 AM)
-- Purpose: Total spend analysis by vendor, category, project, period
-- Grain: vendor × material_category × project × year_month

CREATE OR REPLACE MATERIALIZED VIEW procurement_dev.semantic.cube_procurement_spend
SCHEDULE CRON '0 6 * * *'
COMMENT 'Daily-refreshed cube: Procurement spend by vendor, category, project, and period'
AS
SELECT
    -- Time dimensions
    d.year_number,
    d.quarter_name,
    d.month_name,
    d.fiscal_year,
    d.fiscal_quarter,
    CONCAT(d.year_number, '-', LPAD(d.month_number, 2, '0')) AS year_month,
    
    -- Vendor dimensions
    v.vendor_id,
    v.vendor_name,
    v.vendor_type,
    v.sector_specialization,
    v.country                     AS vendor_country,
    v.risk_category,
    v.performance_rating,
    
    -- Material / Category dimensions
    m.product_category,
    m.sub_category,
    m.commodity_code,
    m.material_name,
    m.material_level,
    m.unit_of_measure,
    m.strategic_item,
    
    -- Project dimensions
    p.project_id,
    p.project_name,
    p.project_type,
    p.sector                      AS project_sector,
    p.country                     AS project_country,
    p.project_status,
    p.client_name,
    
    -- Sector
    s.sector_name,
    s.sub_sector_name,
    
    -- PO Type breakdown
    f.po_type,
    f.po_status,
    f.priority_flag,
    
    -- Spend measures
    COUNT(DISTINCT f.po_id)                              AS po_count,
    COUNT(f.line_id)                                     AS po_line_count,
    SUM(f.quantity_ordered)                              AS total_qty_ordered,
    SUM(f.quantity_received)                             AS total_qty_received,
    SUM(f.line_amount_usd)                               AS gross_spend_usd,
    SUM(f.tax_amount_usd)                                AS total_tax_usd,
    SUM(f.discount_amount_usd)                           AS total_discount_usd,
    SUM(f.total_line_amount_usd)                         AS net_spend_usd,
    SUM(f.received_value_usd)                            AS received_spend_usd,
    SUM(f.outstanding_value_usd)                         AS outstanding_spend_usd,
    
    -- Performance measures
    AVG(f.days_to_delivery)                              AS avg_days_to_delivery,
    AVG(f.delivery_variance_days)                        AS avg_delivery_variance_days,
    SUM(CASE WHEN f.delivery_variance_days > 0 THEN 1 ELSE 0 END) AS late_deliveries,
    SUM(CASE WHEN f.delivery_variance_days <= 0 THEN 1 ELSE 0 END) AS on_time_deliveries,
    ROUND(SUM(CASE WHEN f.delivery_variance_days <= 0 THEN 1 ELSE 0 END) 
          / COUNT(*) * 100, 2)                           AS on_time_delivery_pct,
    AVG(f.fulfillment_pct)                               AS avg_fulfillment_pct,
    SUM(f.rejection_quantity)                            AS total_rejected_qty,
    AVG(f.rejection_rate_pct)                            AS avg_rejection_rate_pct,

    -- Emergency spend
    SUM(CASE WHEN f.priority_flag = 'CRITICAL' 
              THEN f.total_line_amount_usd ELSE 0 END)   AS emergency_spend_usd,
    ROUND(SUM(CASE WHEN f.priority_flag = 'CRITICAL' 
                    THEN f.total_line_amount_usd ELSE 0 END)
          / NULLIF(SUM(f.total_line_amount_usd), 0) * 100, 2) AS emergency_spend_pct

FROM procurement_dev.gold.fact_purchase_orders  f
JOIN procurement_dev.gold.dim_vendor            v  ON f.vendor_key      = v.vendor_key   AND v.is_current
JOIN procurement_dev.gold.dim_material          m  ON f.material_key    = m.material_key AND m.is_current
JOIN procurement_dev.gold.dim_project           p  ON f.project_key     = p.project_key  AND p.is_current
JOIN procurement_dev.gold.dim_date              d  ON f.order_date_key  = d.date_key
JOIN procurement_dev.gold.dim_sector            s  ON f.sector_key      = s.sector_key
GROUP BY ALL;
```

---

### CUBE 2: `04_cube_vendor_performance.sql` — Vendor Scorecard

```sql
CREATE OR REPLACE MATERIALIZED VIEW procurement_dev.semantic.cube_vendor_performance
SCHEDULE CRON '0 7 * * *'
COMMENT 'Vendor performance scorecard — delivery, quality, commercial, HSE combined'
AS
SELECT
    v.vendor_id,
    v.vendor_name,
    v.vendor_type,
    v.sector_specialization,
    v.country,
    v.risk_category,
    v.performance_rating          AS master_data_rating,

    -- Delivery performance (from fact_purchase_orders)
    COUNT(DISTINCT po.po_id)      AS total_pos_issued,
    SUM(po.total_line_amount_usd) AS total_po_value_usd,
    AVG(po.delivery_variance_days) AS avg_delivery_variance,
    ROUND(SUM(CASE WHEN po.delivery_variance_days <= 0 THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*),0) * 100, 2) AS po_on_time_pct,
    AVG(po.fulfillment_pct)       AS avg_fulfillment_pct,
    AVG(po.rejection_rate_pct)    AS avg_rejection_rate_pct,

    -- Invoice performance (from fact_invoices)
    COUNT(DISTINCT inv.invoice_id) AS total_invoices,
    SUM(inv.gross_invoice_amount)  AS total_invoiced_usd,
    SUM(inv.disputed_amount)       AS total_disputed_usd,
    ROUND(SUM(inv.disputed_amount) / NULLIF(SUM(inv.gross_invoice_amount),0) * 100, 2) AS dispute_rate_pct,
    AVG(inv.days_outstanding)      AS avg_payment_days,

    -- Contract performance (from fact_contracts)
    COUNT(DISTINCT ct.contract_id) AS total_contracts,
    SUM(ct.contracted_amount)      AS total_contracted_usd,
    SUM(ct.paid_amount)            AS total_paid_usd,
    AVG(ct.completion_pct)         AS avg_completion_pct,

    -- Vendor performance log (from dim_vendor_performance if tracked)
    -- Composite score calculation
    ROUND(
        (AVG(po.fulfillment_pct) * 0.25) +
        ((100 - AVG(po.rejection_rate_pct)) * 0.25) +
        ((100 - ROUND(SUM(inv.disputed_amount)/NULLIF(SUM(inv.gross_invoice_amount),0)*100,2)) * 0.25) +
        (AVG(ct.completion_pct) * 0.25)
    , 2)                           AS composite_performance_score,

    -- Classification
    CASE
        WHEN ROUND((AVG(po.fulfillment_pct)*0.25)+((100-AVG(po.rejection_rate_pct))*0.25)+
             ((100-ROUND(SUM(inv.disputed_amount)/NULLIF(SUM(inv.gross_invoice_amount),0)*100,2))*0.25)+
             (AVG(ct.completion_pct)*0.25),2) >= 85 THEN 'PREFERRED'
        WHEN ROUND((AVG(po.fulfillment_pct)*0.25)+((100-AVG(po.rejection_rate_pct))*0.25)+
             ((100-ROUND(SUM(inv.disputed_amount)/NULLIF(SUM(inv.gross_invoice_amount),0)*100,2))*0.25)+
             (AVG(ct.completion_pct)*0.25),2) >= 70 THEN 'APPROVED'
        WHEN ROUND((AVG(po.fulfillment_pct)*0.25)+((100-AVG(po.rejection_rate_pct))*0.25)+
             ((100-ROUND(SUM(inv.disputed_amount)/NULLIF(SUM(inv.gross_invoice_amount),0)*100,2))*0.25)+
             (AVG(ct.completion_pct)*0.25),2) >= 50 THEN 'CONDITIONAL'
        ELSE 'DELIST_CANDIDATE'
    END                             AS recommended_status

FROM procurement_dev.gold.dim_vendor v
LEFT JOIN procurement_dev.gold.fact_purchase_orders po ON v.vendor_key = po.vendor_key
LEFT JOIN procurement_dev.gold.fact_invoices        inv ON v.vendor_key = inv.vendor_key
LEFT JOIN procurement_dev.gold.fact_contracts       ct  ON v.vendor_key = ct.contractor_key AND ct.is_current
WHERE v.is_current = TRUE
GROUP BY ALL;
```

---

### CUBE 3: `04_cube_contract_performance.sql` — Contract Health Dashboard
```sql
-- [COPILOT: Generate full SQL]
-- Metrics: contract_id, project, contractor, type, original_value, revised_value,
--          cost_growth_pct, days_overrun, completion_pct, retention_held,
--          payment_progress_pct, variation_count, risk_flag (over 10% cost growth OR >30 days late)
```

### CUBE 4: `04_cube_project_cost_variance.sql` — Budget vs Actual vs Forecast
```sql
-- [COPILOT: Generate full SQL]
-- Metrics by project × WBS × cost_type × period:
--   approved_budget, committed_cost, actual_cost, forecast_cost,
--   budget_variance, schedule_variance, cost_performance_index (CPI),
--   estimate_at_completion (EAC), variance_at_completion (VAC),
--   percent_complete, to_complete_performance_index (TCPI),
--   overrun_flag = (forecast_cost > approved_budget)
```

### CUBE 5: `04_cube_po_cycle_time.sql` — Procurement Cycle Time Analytics
```sql
-- [COPILOT: Generate full SQL]
-- Metrics: avg_days_from_requisition_to_po, avg_days_po_to_delivery,
--          avg_total_cycle_time, by sector/vendor/category/priority
--          percentile_25/50/75/95 of cycle time,
--          count_overdue_pos, count_critical_overdue
```

### CUBE 6: `04_cube_invoice_aging.sql` — AP Invoice Aging + Cash Flow
```sql
-- [COPILOT: Generate full SQL]
-- Aging buckets: CURRENT (0-30), 31-60, 61-90, 91-120, 120+ DAYS
-- By vendor, project, invoice_type
-- Metrics: total_outstanding, disputed_amount, aging_bucket_usd,
--          overdue_amount, early_payment_discount_available,
--          projected_cash_outflow_next_30_60_90_days
```

### CUBE 7: `04_cube_material_consumption.sql` — Material Usage by Project/Site
```sql
-- [COPILOT: Generate full SQL]
-- Metrics: material_id, project, period, opening_stock, receipts,
--          issued_to_site, closing_stock, stock_value_usd,
--          consumption_vs_plan_pct, wastage_pct,
--          reorder_flag = (closing_stock < minimum_order_quantity)
```

### CUBE 8: `04_cube_category_spend.sql` — Spend by Category + Market Benchmark
```sql
-- [COPILOT: Generate full SQL]
-- Metrics: product_category, sub_category, commodity_code,
--          total_spend_usd, avg_unit_price, min_unit_price, max_unit_price,
--          price_std_dev, coefficient_of_variation,
--          number_of_vendors, spend_concentration_top3_vendors_pct,
--          yoy_spend_growth_pct, potential_consolidation_savings
```

### CUBE 9: `04_cube_budget_utilization.sql` — Budget Burn + Cash Flow Forecast
```sql
-- [COPILOT: Generate full SQL]
-- Metrics: project × cost_center × period:
--   approved_budget, committed, actual, forecast_to_complete,
--   total_at_completion, remaining_budget, utilization_pct,
--   burn_rate (monthly), months_to_budget_exhaustion,
--   contingency_consumed_pct
```

### CUBE 10: `04_cube_sales_revenue.sql` — Sales & Revenue Analytics
```sql
-- [COPILOT: Generate full SQL]
-- At PRODUCT level AND ITEM level (dual grain):
-- product_category, product_name, item_name, project, client,
-- region, period, sector
-- Metrics: quantity_sold_product_level, quantity_sold_item_level,
--          revenue_product, revenue_item, cogs, gross_margin,
--          gross_margin_pct, discount_pct, net_revenue,
--          ytd_revenue, revenue_vs_budget_pct
```

### CUBE 11: `04_cube_procurement_savings.sql` — Cost Savings Tracking
```sql
-- [COPILOT: Generate full SQL]
-- Compare actual unit prices vs market benchmark prices
-- By negotiated_savings = (benchmark_price - actual_price) * quantity
-- By category, vendor, project, period
-- savings_type: PRICE_NEGOTIATION / VOLUME_DISCOUNT / EARLY_PAYMENT / SPECIFICATION_CHANGE
-- target_savings_usd vs actual_savings_usd vs realized_savings_pct
```

### CUBE 12: `04_cube_supplier_risk.sql` — Supply Chain Risk Dashboard
```sql
-- [COPILOT: Generate full SQL]
-- risk_score per vendor per project = weighted(
--   financial_risk + geopolitical_risk + concentration_risk +
--   delivery_risk + quality_risk + compliance_risk)
-- spend_at_risk_usd, single_source_items, critical_path_vendors,
-- blacklisted_transactions, expired_prequalification_spend,
-- high_risk_country_spend
```

### CUBE 13: `04_cube_milestone_cost.sql` — Project Milestone Cost Tracker
```sql
-- [COPILOT: Generate full SQL]
-- One row per milestone per contract
-- planned_milestone_date, actual_milestone_date, milestone_variance_days,
-- planned_payment, actual_certified_payment, payment_variance,
-- cumulative_completion, s_curve data points (monthly planned vs actual)
-- milestone_status: PENDING/ACHIEVED_ON_TIME/ACHIEVED_LATE/MISSED
```

### CUBE 14: `04_cube_inventory_turnover.sql` — Inventory Efficiency
```sql
-- [COPILOT: Generate full SQL]
-- inventory_turnover_ratio = annual_cogs / avg_inventory_value
-- days_inventory_outstanding (DIO)
-- slow_moving_flag = (last_movement_date < today - 90 days)
-- obsolete_flag = (last_movement_date < today - 180 days)
-- excess_stock_value (stock > 3 months consumption rate)
-- by material_category, site, project
```

### CUBE 15: `04_cube_og_equipment.sql` — O&G Equipment Procurement KPIs
```sql
-- [COPILOT: Generate full SQL]
-- Specific to OIL_GAS sector items: equipment, instruments, valves, pipes
-- critical_long_lead_items: items with lead_time > 90 days
-- committed_not_received, expediting_flag (required_date within 30 days)
-- inspection_backlog, ATEX/hazardous_material_spend,
-- equipment_procurement_lead_time_vs_benchmark,
-- plant_commissioning_readiness_pct (% of critical equipment received)
```

---

## UNITY CATALOG SETUP

### File: `databricks/setup/00_unity_catalog_setup.py`

```python
"""
Unity Catalog Setup Script
Run ONCE as admin before running any pipelines
Sets up catalogs, schemas, volumes, grants, and tags
"""

# %sql
# -- ─── CATALOG STRUCTURE ──────────────────────────────────────────────
# -- Three catalogs: dev, staging, production
# CREATE CATALOG IF NOT EXISTS procurement_dev;
# CREATE CATALOG IF NOT EXISTS procurement_staging;
# CREATE CATALOG IF NOT EXISTS procurement_prod;
#
# -- ─── SCHEMAS PER CATALOG ─────────────────────────────────────────────
# USE CATALOG procurement_dev;
# CREATE SCHEMA IF NOT EXISTS bronze COMMENT 'Raw ingested data — no transforms';
# CREATE SCHEMA IF NOT EXISTS silver COMMENT 'Cleansed and conformed data';
# CREATE SCHEMA IF NOT EXISTS gold   COMMENT 'Star schema — dimensions and facts';
# CREATE SCHEMA IF NOT EXISTS semantic COMMENT 'Data cubes and materialized views';
# CREATE SCHEMA IF NOT EXISTS dq     COMMENT 'Data quality metrics and quarantine';
#
# -- ─── VOLUMES (for file ingestion) ────────────────────────────────────
# CREATE VOLUME IF NOT EXISTS procurement_dev.bronze.raw_files
#   COMMENT 'Landing zone for Excel source files';
#
# -- ─── ROW/COLUMN SECURITY ─────────────────────────────────────────────
# -- Finance team sees all amounts
# -- Procurement team cannot see vendor bank account details
# CREATE ROW FILTER finance_only ON procurement_dev.gold.fact_invoices
#   USING (CURRENT_USER() IN (SELECT user_email FROM procurement_dev.gold.dim_employee
#                              WHERE department = 'FINANCE'));
#
# -- Mask bank account for non-finance
# ALTER TABLE procurement_dev.gold.dim_vendor
#   ALTER COLUMN bank_account
#   SET MASK vendor_bank_mask
#   USING COLUMNS (vendor_id);
#
# -- ─── GRANTS ──────────────────────────────────────────────────────────
# GRANT USE CATALOG ON CATALOG procurement_dev TO `procurement_team`;
# GRANT USE SCHEMA  ON SCHEMA procurement_dev.gold TO `procurement_team`;
# GRANT SELECT      ON ALL TABLES IN SCHEMA procurement_dev.gold TO `procurement_team`;
# GRANT SELECT      ON ALL TABLES IN SCHEMA procurement_dev.semantic TO `procurement_team`;
#
# -- ─── TAGS for data discovery ─────────────────────────────────────────
# ALTER TABLE procurement_dev.gold.fact_purchase_orders
#   SET TAGS ('domain'='procurement', 'pii'='false', 'criticality'='high');
# ALTER TABLE procurement_dev.gold.dim_vendor
#   SET TAGS ('domain'='vendor', 'pii'='true', 'criticality'='high');
```

---

## PIPELINE CONFIGURATIONS (JSON)

### File: `databricks/pipelines/pipeline_bronze.json`
```json
{
  "name": "procurement_bronze_pipeline",
  "catalog": "procurement_dev",
  "target": "bronze",
  "configuration": {
    "pipeline.catalog": "procurement_dev",
    "pipeline.env": "dev",
    "spark.databricks.delta.schema.autoMerge.enabled": "true"
  },
  "libraries": [
    {"notebook": {"path": "/databricks/bronze/01_bronze_pipeline"}}
  ],
  "serverless": true,
  "channel": "CURRENT",
  "continuous": false,
  "development": false,
  "photon": true,
  "trigger": {"manual_trigger": {}}
}
```

### File: `databricks/pipelines/pipeline_silver.json`
```json
{
  "name": "procurement_silver_pipeline",
  "catalog": "procurement_dev",
  "target": "silver",
  "libraries": [
    {"notebook": {"path": "/databricks/silver/02_silver_pipeline"}}
  ],
  "serverless": true,
  "photon": true
}
```

### File: `databricks/pipelines/pipeline_gold.json`
```json
{
  "name": "procurement_gold_pipeline",
  "catalog": "procurement_dev",
  "target": "gold",
  "libraries": [
    {"notebook": {"path": "/databricks/gold/03_gold_pipeline"}}
  ],
  "serverless": true,
  "photon": true
}
```

---

## WORKFLOW ORCHESTRATION

### File: `databricks/orchestration/workflow_demo.json`

```json
{
  "name": "procurement_demo_workflow",
  "tasks": [
    {
      "task_key": "generate_test_data",
      "description": "Generate all 15 Excel test data files (100K records each)",
      "notebook_task": {
        "notebook_path": "/data_generators/00_master_generator",
        "source": "WORKSPACE"
      },
      "libraries": [
        {"pypi": {"package": "faker>=24.0"}},
        {"pypi": {"package": "openpyxl>=3.1"}},
        {"pypi": {"package": "pandas>=2.0"}},
        {"pypi": {"package": "numpy>=1.26"}}
      ],
      "job_cluster_key": "single_node_cluster"
    },
    {
      "task_key": "upload_to_volume",
      "description": "Upload generated files to ADLS Volume",
      "depends_on": [{"task_key": "generate_test_data"}],
      "notebook_task": {
        "notebook_path": "/databricks/setup/upload_to_volume"
      },
      "job_cluster_key": "single_node_cluster"
    },
    {
      "task_key": "run_bronze_pipeline",
      "description": "Pipeline 1: Raw ingestion → bronze.raw_* (15 tables)",
      "depends_on": [{"task_key": "upload_to_volume"}],
      "pipeline_task": {
        "pipeline_id": "{{bronze_pipeline_id}}",
        "full_refresh": false
      }
    },
    {
      "task_key": "run_silver_pipeline",
      "description": "Pipeline 2: Cleanse → silver.clean_* (15 tables + DQ)",
      "depends_on": [{"task_key": "run_bronze_pipeline"}],
      "pipeline_task": {
        "pipeline_id": "{{silver_pipeline_id}}",
        "full_refresh": false
      }
    },
    {
      "task_key": "run_gold_pipeline",
      "description": "Pipeline 3: Star schema → gold.dim_* + gold.fact_* (16 tables)",
      "depends_on": [{"task_key": "run_silver_pipeline"}],
      "pipeline_task": {
        "pipeline_id": "{{gold_pipeline_id}}",
        "full_refresh": false
      }
    },
    {
      "task_key": "refresh_semantic_cubes",
      "description": "Refresh all 15 materialized view cubes in semantic schema",
      "depends_on": [{"task_key": "run_gold_pipeline"}],
      "sql_task": {
        "queries": [
          "REFRESH MATERIALIZED VIEW procurement_dev.semantic.cube_procurement_spend",
          "REFRESH MATERIALIZED VIEW procurement_dev.semantic.cube_vendor_performance",
          "REFRESH MATERIALIZED VIEW procurement_dev.semantic.cube_contract_performance",
          "REFRESH MATERIALIZED VIEW procurement_dev.semantic.cube_project_cost_variance",
          "REFRESH MATERIALIZED VIEW procurement_dev.semantic.cube_po_cycle_time",
          "REFRESH MATERIALIZED VIEW procurement_dev.semantic.cube_invoice_aging",
          "REFRESH MATERIALIZED VIEW procurement_dev.semantic.cube_material_consumption",
          "REFRESH MATERIALIZED VIEW procurement_dev.semantic.cube_category_spend",
          "REFRESH MATERIALIZED VIEW procurement_dev.semantic.cube_budget_utilization",
          "REFRESH MATERIALIZED VIEW procurement_dev.semantic.cube_sales_revenue",
          "REFRESH MATERIALIZED VIEW procurement_dev.semantic.cube_procurement_savings",
          "REFRESH MATERIALIZED VIEW procurement_dev.semantic.cube_supplier_risk",
          "REFRESH MATERIALIZED VIEW procurement_dev.semantic.cube_milestone_cost",
          "REFRESH MATERIALIZED VIEW procurement_dev.semantic.cube_inventory_turnover",
          "REFRESH MATERIALIZED VIEW procurement_dev.semantic.cube_og_equipment"
        ],
        "warehouse_id": "{{sql_warehouse_id}}"
      }
    },
    {
      "task_key": "validate_pipeline",
      "description": "Run data quality and count assertions",
      "depends_on": [{"task_key": "refresh_semantic_cubes"}],
      "notebook_task": {
        "notebook_path": "/tests/test_bronze_counts"
      },
      "job_cluster_key": "single_node_cluster"
    }
  ],
  "job_clusters": [
    {
      "job_cluster_key": "single_node_cluster",
      "new_cluster": {
        "spark_version": "14.3.x-scala2.12",
        "node_type_id": "Standard_DS4_v2",
        "num_workers": 0,
        "spark_conf": {"spark.master": "local[*]"},
        "custom_tags": {"project": "procurement-lakehouse", "env": "demo"}
      }
    }
  ],
  "max_concurrent_runs": 1,
  "schedule": {
    "quartz_cron_expression": "0 0 6 * * ?",
    "timezone_id": "UTC"
  },
  "email_notifications": {
    "on_failure": ["data-engineering@company.com"],
    "on_success": ["data-engineering@company.com"]
  }
}
```

---

## TEST VALIDATION

### File: `tests/test_bronze_counts.py`

```python
"""
Validation tests — run after each pipeline to assert data quality
"""
from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
CATALOG = "procurement_dev"
ERRORS = []

def assert_count(table, min_count=95000, max_count=105000):
    """Assert table has approximately 100K records."""
    count = spark.table(f"{CATALOG}.bronze.{table}").count()
    if not (min_count <= count <= max_count):
        ERRORS.append(f"FAIL: {table} has {count:,} records (expected ~100K)")
    else:
        print(f"  PASS: {table} = {count:,} records")

def assert_no_nulls(table, column):
    null_count = spark.table(f"{CATALOG}.silver.{table}") \
                      .filter(f"{column} IS NULL").count()
    if null_count > 0:
        ERRORS.append(f"FAIL: {CATALOG}.silver.{table}.{column} has {null_count} NULLs")
    else:
        print(f"  PASS: silver.{table}.{column} — no NULLs")

# Bronze count assertions
print("=== BRONZE LAYER VALIDATION ===")
for t in ["raw_vendors","raw_projects","raw_materials","raw_employees",
          "raw_purchase_orders","raw_po_line_items","raw_contracts",
          "raw_contract_items","raw_invoices","raw_goods_receipts",
          "raw_project_budgets","raw_project_actuals","raw_sales_orders",
          "raw_inventory","raw_vendor_performance"]:
    assert_count(t)

# Silver PK integrity
print("\n=== SILVER LAYER VALIDATION ===")
assert_no_nulls("clean_vendors", "vendor_id")
assert_no_nulls("clean_purchase_orders", "po_id")
assert_no_nulls("clean_contracts", "contract_id")
assert_no_nulls("clean_invoices", "invoice_id")

# Gold referential integrity
print("\n=== GOLD LAYER VALIDATION ===")
orphan_pos = spark.sql(f"""
    SELECT COUNT(*) AS cnt FROM {CATALOG}.gold.fact_purchase_orders f
    LEFT JOIN {CATALOG}.gold.dim_vendor v ON f.vendor_key = v.vendor_key
    WHERE v.vendor_key IS NULL
""").collect()[0]["cnt"]
if orphan_pos > 0:
    ERRORS.append(f"FAIL: {orphan_pos} fact_purchase_orders rows with no dim_vendor match")

# Cube record counts
print("\n=== SEMANTIC LAYER VALIDATION ===")
for cube in ["cube_procurement_spend","cube_vendor_performance",
             "cube_project_cost_variance","cube_invoice_aging","cube_sales_revenue"]:
    count = spark.table(f"{CATALOG}.semantic.{cube}").count()
    print(f"  INFO: {cube} = {count:,} rows")

if ERRORS:
    print(f"\n{'='*60}")
    print(f"VALIDATION FAILED — {len(ERRORS)} errors:")
    for e in ERRORS:
        print(f"  {e}")
    raise AssertionError(f"{len(ERRORS)} validation errors found")
else:
    print("\nALL VALIDATIONS PASSED")
```

---

## DEMO RUN INSTRUCTIONS

### Step 1: Clone and setup (5 minutes)
```bash
git clone <your-repo>
cd procurement-lakehouse
pip install faker pandas openpyxl numpy databricks-sdk
```

### Step 2: Generate all test data (10 minutes)
```bash
cd data_generators
python 00_master_generator.py
# Expected output: 15 xlsx files, each with exactly 100,000 rows
# Total data: ~1.5M rows across all files
```

### Step 3: Upload to Databricks Volume
```bash
# Using Databricks CLI
databricks fs cp data/raw/*.xlsx dbfs:/Volumes/procurement_dev/bronze/raw_files/
```

### Step 4: Run Unity Catalog setup
```
In Databricks: Run databricks/setup/00_unity_catalog_setup.py as admin
```

### Step 5: Create and run 3 DLT pipelines
```
Pipeline 1 (Bronze): databricks/pipelines/pipeline_bronze.json
Pipeline 2 (Silver): databricks/pipelines/pipeline_silver.json
Pipeline 3 (Gold):   databricks/pipelines/pipeline_gold.json
Run in sequence via Databricks UI or Databricks Asset Bundles (DAB):
  databricks bundle deploy --target dev
  databricks bundle run procurement_demo_workflow
```

### Step 6: Validate
```bash
databricks jobs run-now --job-name "procurement_demo_workflow"
# Watch for: ALL VALIDATIONS PASSED
```

### Step 7: Connect Power BI
```
1. In Power BI Desktop: Get Data → Databricks → Databricks SQL
2. Connect to SQL Warehouse endpoint
3. Navigate to: procurement_dev → semantic
4. Import all 15 cube_* materialized views
5. Relationships are pre-defined via cube SQL — no manual joins needed
```

---

## COPILOT: FINAL GENERATION CHECKLIST

When you generate each file, check ALL of the following:

- [ ] Every generator produces exactly 100,000 rows
- [ ] All FK columns reference valid IDs from parent tables
- [ ] All date columns use realistic date ranges (2019–2024 for historical; up to 2027 for future)
- [ ] All monetary columns use realistic ranges per sector (oil & gas = larger values)
- [ ] All percentage columns are bounded 0–100
- [ ] Status columns use only the enum values listed in the schema
- [ ] Bronze pipeline: every table has all 5 metadata columns
- [ ] Silver pipeline: every table has ≥3 expect_or_drop + ≥4 expect_or_quarantine rules
- [ ] Gold dimensions: all 10 dimensions have SCD2 columns (eff_start/end, is_current, record_hash)
- [ ] Gold facts: all 6 facts have ≥4 FK lookups to dimensions
- [ ] All 15 cubes are MATERIALIZED VIEWS (not regular views)
- [ ] All cubes have SCHEDULE CRON clause for auto-refresh
- [ ] Workflow JSON has all 7 tasks in dependency order
- [ ] Test file validates record counts ≥95K for all 15 bronze tables
- [ ] No hardcoded file paths — use spark.conf.get for all configurable params
- [ ] All notebooks have docstrings with purpose, inputs, outputs, dependencies

---

*End of Copilot Instructions — Construction & Oil/Gas Procurement Lakehouse v1.0*
*Generated for: VS Code GitHub Copilot | Databricks Runtime 14.3 LTS*
