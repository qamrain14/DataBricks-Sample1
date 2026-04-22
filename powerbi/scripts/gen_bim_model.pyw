"""Generate procurement_model.bim (Tabular Editor JSON) for Power BI."""
import json, os, sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "."))
from connection_config import ALL_TABLES, GOLD_DIMENSIONS, GOLD_FACTS, SEMANTIC_CUBES, CONNECTION


def pascal(s):
    return "".join(w.capitalize() for w in s.split("_"))


def make_column(col_name):
    pc = pascal(col_name)
    return {
        "name": pc,
        "dataType": "string",
        "sourceColumn": col_name,
        "annotations": [{"name": "SummarizationType", "value": "None"}],
    }


def make_table(key, meta):
    cols = [c for c in meta.columns if not c.startswith("_")]
    pq_path = f"powerbi/connections/{key}.pq"

    # Read actual .pq content
    pq_file = os.path.join(os.path.dirname(__file__), "..", "connections", f"{key}.pq")
    if os.path.exists(pq_file):
        with open(pq_file, "r", encoding="utf-8") as f:
            expression = f.read()
    else:
        expression = f'// Missing: {pq_file}'

    t = {
        "name": pascal(key),
        "columns": [make_column(c) for c in cols],
        "partitions": [
            {
                "name": "Partition",
                "dataView": "full",
                "source": {"type": "m", "expression": expression.split("\n")},
            }
        ],
        "annotations": [
            {"name": "PBI_NavigationStepName", "value": "Navigation"},
            {"name": "SourceTable", "value": f"{meta.schema}.{meta.table}"},
        ],
    }
    return t


# --- Relationships ---
RELATIONSHIPS = [
    # fact -> dim via keys
    ("FactPurchaseOrders", "VendorId", "DimVendor", "VendorId"),
    ("FactPurchaseOrders", "ProjectId", "DimProject", "ProjectId"),
    ("FactInvoices", "VendorId", "DimVendor", "VendorId"),
    ("FactInvoices", "ProjectId", "DimProject", "ProjectId"),
    ("FactInvoices", "ContractId", "DimContract", "ContractId"),
    ("FactGoodsReceipts", "VendorId", "DimVendor", "VendorId"),
    ("FactGoodsReceipts", "MaterialId", "DimMaterial", "MaterialId"),
    ("FactGoodsReceipts", "ProjectId", "DimProject", "ProjectId"),
    ("FactProjectCosts", "ProjectId", "DimProject", "ProjectId"),
    ("FactProjectActuals", "TransactionDate", "DimDate", "DateKey"),
    ("FactVendorPerformance", "VendorId", "DimVendor", "VendorId"),
    ("FactVendorPerformance", "ProjectId", "DimProject", "ProjectId"),
    ("FactInventory", "MaterialId", "DimMaterial", "MaterialId"),
    ("FactContracts", "VendorId", "DimVendor", "VendorId"),
    ("FactContracts", "ProjectId", "DimProject", "ProjectId"),
    ("FactSales", "OrderDate", "DimDate", "DateKey"),
    # cubes -> dims
    ("CubeProcurementSpend", "VendorId", "DimVendor", "VendorId"),
    ("CubeProcurementSpend", "ProjectId", "DimProject", "ProjectId"),
    ("CubeVendorPerformance", "VendorId", "DimVendor", "VendorId"),
    ("CubeContractStatus", "VendorId", "DimVendor", "VendorId"),
    ("CubeContractStatus", "ProjectId", "DimProject", "ProjectId"),
    ("CubeInvoiceAging", "VendorId", "DimVendor", "VendorId"),
    ("CubeInvoiceAging", "ProjectId", "DimProject", "ProjectId"),
    ("CubeProjectCosts", "ProjectId", "DimProject", "ProjectId"),
    ("CubeGoodsReceiptQuality", "VendorId", "DimVendor", "VendorId"),
    ("CubeGoodsReceiptQuality", "MaterialId", "DimMaterial", "MaterialId"),
    ("CubeBudgetVsActual", "ProjectId", "DimProject", "ProjectId"),
    ("CubeVendorRisk", "VendorId", "DimVendor", "VendorId"),
    ("CubeCostVariance", "ProjectId", "DimProject", "ProjectId"),
    ("CubeSpendBySector", "VendorId", "DimVendor", "VendorId"),
    ("CubeDeliveryPerformance", "VendorId", "DimVendor", "VendorId"),
    ("CubeProcurementEfficiency", "VendorId", "DimVendor", "VendorId"),
    ("CubeProjectTimeline", "ProjectId", "DimProject", "ProjectId"),
]

# --- DAX Measures ---
MEASURES = [
    # Spend
    {"name": "Total Spend", "table": "FactPurchaseOrders", "expression": "SUM(FactPurchaseOrders[TotalValue])", "formatString": "$#,##0.00"},
    {"name": "Total PO Count", "table": "FactPurchaseOrders", "expression": "COUNTROWS(FactPurchaseOrders)", "formatString": "#,##0"},
    {"name": "Avg PO Value", "table": "FactPurchaseOrders", "expression": "AVERAGE(FactPurchaseOrders[TotalValue])", "formatString": "$#,##0.00"},
    # Invoices
    {"name": "Total Invoiced", "table": "FactInvoices", "expression": "SUM(FactInvoices[GrossAmount])", "formatString": "$#,##0.00"},
    {"name": "Total Paid", "table": "FactInvoices", "expression": "SUM(FactInvoices[PaidAmount])", "formatString": "$#,##0.00"},
    {"name": "Overdue Amount", "table": "FactInvoices", "expression": "CALCULATE(SUM(FactInvoices[GrossAmount]), FactInvoices[PaymentStatus] = \"Overdue\")", "formatString": "$#,##0.00"},
    {"name": "Avg Days to Pay", "table": "FactInvoices", "expression": "AVERAGE(FactInvoices[DaysToPay])", "formatString": "#,##0.0"},
    # Project Costs
    {"name": "Total Budget", "table": "FactProjectCosts", "expression": "SUM(FactProjectCosts[BudgetAmount])", "formatString": "$#,##0.00"},
    {"name": "Total Actual Cost", "table": "FactProjectCosts", "expression": "SUM(FactProjectCosts[ActualAmount])", "formatString": "$#,##0.00"},
    {"name": "Budget Variance", "table": "FactProjectCosts", "expression": "[Total Budget] - [Total Actual Cost]", "formatString": "$#,##0.00"},
    {"name": "Budget Utilization %", "table": "FactProjectCosts", "expression": "DIVIDE([Total Actual Cost], [Total Budget], 0)", "formatString": "0.0%"},
    # Vendor Performance
    {"name": "Avg Vendor Score", "table": "FactVendorPerformance", "expression": "AVERAGE(FactVendorPerformance[OverallScore])", "formatString": "0.00"},
    {"name": "Vendor Count", "table": "DimVendor", "expression": "COUNTROWS(DimVendor)", "formatString": "#,##0"},
    # Goods Receipts
    {"name": "Total Received Qty", "table": "FactGoodsReceipts", "expression": "SUM(FactGoodsReceipts[QuantityReceived])", "formatString": "#,##0"},
    {"name": "Avg Acceptance Rate", "table": "FactGoodsReceipts", "expression": "AVERAGE(FactGoodsReceipts[AcceptanceRate])", "formatString": "0.0%"},
    # Contracts
    {"name": "Total Contract Value", "table": "FactContracts", "expression": "SUM(FactContracts[ContractValue])", "formatString": "$#,##0.00"},
    {"name": "Active Contracts", "table": "FactContracts", "expression": "CALCULATE(COUNTROWS(FactContracts), FactContracts[ContractStatus] = \"Active\")", "formatString": "#,##0"},
    # Sales
    {"name": "Total Revenue", "table": "FactSales", "expression": "SUM(FactSales[Revenue])", "formatString": "$#,##0.00"},
    {"name": "Total COGS", "table": "FactSales", "expression": "SUM(FactSales[Cogs])", "formatString": "$#,##0.00"},
    {"name": "Gross Margin", "table": "FactSales", "expression": "SUM(FactSales[GrossMargin])", "formatString": "$#,##0.00"},
    {"name": "Margin %", "table": "FactSales", "expression": "DIVIDE([Gross Margin], [Total Revenue], 0)", "formatString": "0.0%"},
    # Inventory
    {"name": "Net Inventory Flow", "table": "FactInventory", "expression": "SUM(FactInventory[Quantity])", "formatString": "#,##0"},
    {"name": "Inventory Value", "table": "FactInventory", "expression": "SUM(FactInventory[TotalValue])", "formatString": "$#,##0.00"},
    # KPIs
    {"name": "Project Count", "table": "DimProject", "expression": "COUNTROWS(DimProject)", "formatString": "#,##0"},
    {"name": "Over Budget Projects", "table": "CubeProjectCosts", "expression": "CALCULATE(COUNTROWS(CubeProjectCosts), CubeProjectCosts[IsOverBudget] = TRUE())", "formatString": "#,##0"},
]


def make_relationship(idx, from_table, from_col, to_table, to_col):
    return {
        "name": f"{from_table}_{from_col}_{to_table}_{to_col}",
        "fromTable": from_table,
        "fromColumn": from_col,
        "toTable": to_table,
        "toColumn": to_col,
        "crossFilteringBehavior": "bothDirections" if to_table.startswith("Dim") else "oneDirection",
    }


def make_measure(m):
    obj = {
        "name": m["name"],
        "expression": m["expression"],
    }
    if "formatString" in m:
        obj["formatString"] = m["formatString"]
    return obj


# Build model
tables = []
for key in list(GOLD_DIMENSIONS.keys()) + list(GOLD_FACTS.keys()) + list(SEMANTIC_CUBES.keys()):
    tables.append(make_table(key, ALL_TABLES[key]))

# Add measures to their tables
measure_map = {}
for m in MEASURES:
    measure_map.setdefault(m["table"], []).append(make_measure(m))

for t in tables:
    tname = t["name"]
    if tname in measure_map:
        t["measures"] = measure_map[tname]

relationships = []
for i, (ft, fc, tt, tc) in enumerate(RELATIONSHIPS):
    relationships.append(make_relationship(i, ft, fc, tt, tc))

model = {
    "name": "ProcurementAnalytics",
    "compatibilityLevel": 1550,
    "model": {
        "culture": "en-US",
        "dataAccessOptions": {"legacyRedirects": True, "returnErrorValuesAsNull": True},
        "defaultPowerBIDataSourceVersion": "powerBI_V3",
        "tables": tables,
        "relationships": relationships,
        "annotations": [
            {"name": "PBI_QueryOrder", "value": json.dumps([t["name"] for t in tables])},
            {"name": "PBIDesktopVersion", "value": "2.138.0.0 (24.10)"},
        ],
    },
}

out_dir = os.path.join(os.path.dirname(__file__), "..", "model")
os.makedirs(out_dir, exist_ok=True)
out_path = os.path.join(out_dir, "procurement_model.bim")
with open(out_path, "w", encoding="utf-8") as f:
    json.dump(model, f, indent=2)

print(f"Generated BIM model at {out_path}")
print(f"  Tables: {len(tables)}")
print(f"  Relationships: {len(relationships)}")
print(f"  Measures: {len(MEASURES)}")
