let
    Source = Databricks.Catalogs("dbc-760a206e-c226.cloud.databricks.com", "/sql/1.0/warehouses/7655fa24e271f9d1", [Catalog="workspace", Database="procurement_gold"]),
    Nav1 = Source{[Name="workspace"]}[Data],
    Nav2 = Nav1{[Name="procurement_gold"]}[Data],
    RawTable = Nav2{[Name="fact_project_actuals"]}[Data],
    RemoveInternal = Table.RemoveColumns(RawTable, {"_gold_timestamp"}),
    Renamed = Table.RenameColumns(RemoveInternal, {{"transaction_id", "TransactionId"}, {"po_id", "PoId"}, {"invoice_id", "InvoiceId"}, {"wbs_code", "WbsCode"}, {"cost_type", "CostType"}, {"cost_category", "CostCategory"}, {"cost_status", "CostStatus"}, {"transaction_date", "TransactionDate"}, {"posting_date", "PostingDate"}, {"actual_amount", "ActualAmount"}, {"budget_amount", "BudgetAmount"}, {"variance", "Variance"}, {"fiscal_period", "FiscalPeriod"}})
in
    Renamed