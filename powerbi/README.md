# Power BI Integration — Procurement Analytics

## Overview
Power BI reporting layer for the Databricks Procurement Analytics platform. Connects to Unity Catalog via the native Databricks connector (Import mode).

## Structure
```
powerbi/
├── connections/         # 33 Power Query M files (.pq)
├── docs/
│   └── REPORT_SPEC.md  # 11-page report specification
├── model/
│   └── procurement_model.bim  # Tabular Editor JSON model
├── scripts/
│   ├── connection_config.py   # Table schemas & connection metadata
│   ├── gen_pq_files.pyw       # Generates .pq files from config
│   ├── gen_bim_model.pyw      # Generates BIM model from config
│   ├── odbc_setup.bat         # ODBC DSN registration
│   └── refresh.bat            # Manual refresh automation
└── README.md
```

## Quick Start

### Prerequisites
- Power BI Desktop (free)
- Tabular Editor 2 (free) — optional, for editing BIM model
- Databricks personal access token

### Setup Steps

1. **Open Power BI Desktop** → Get Data → Databricks
2. **Server**: `dbc-760a206e-c226.cloud.databricks.com`
3. **HTTP Path**: `/sql/1.0/warehouses/7655fa24e271f9d1`
4. **Auth**: Personal Access Token
5. **Import tables**: Use the .pq files in `connections/` as query templates
   - In Power Query Editor → New Query → Blank Query → paste .pq content
6. **Build visuals** per `docs/REPORT_SPEC.md`

### Using the BIM Model (Optional)
1. Open `model/procurement_model.bim` in Tabular Editor 2
2. Review tables, relationships, DAX measures
3. Deploy to Power BI Desktop via File → Save to Power BI

### Regenerating Artifacts
From workspace root:
```bash
python powerbi/scripts/gen_pq_files.pyw    # regenerate .pq files
python powerbi/scripts/gen_bim_model.pyw    # regenerate BIM model
```

## Data Model
- **9 Dimensions**: Date, Vendor, Project, Material, Employee, Geography, Contract, Cost Center, Sector
- **9 Facts**: Purchase Orders, Invoices, Goods Receipts, Project Costs, Project Actuals, Vendor Performance, Inventory, Contracts, Sales
- **15 Cubes**: Pre-aggregated semantic layer tables
- **25 DAX Measures**: Spend, revenue, margins, KPIs
- **33 Relationships**: Star schema pattern
