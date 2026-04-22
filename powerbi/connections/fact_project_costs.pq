let
    Source = Databricks.Catalogs("dbc-760a206e-c226.cloud.databricks.com", "/sql/1.0/warehouses/7655fa24e271f9d1", [Catalog="workspace", Database="procurement_gold"]),
    Nav1 = Source{[Name="workspace"]}[Data],
    Nav2 = Nav1{[Name="procurement_gold"]}[Data],
    RawTable = Nav2{[Name="fact_project_costs"]}[Data],
    RemoveInternal = Table.RemoveColumns(RawTable, {"_gold_timestamp"}),
    Renamed = Table.RenameColumns(RemoveInternal, {{"budget_id", "BudgetId"}, {"project_id", "ProjectId"}, {"wbs_element", "WbsElement"}, {"wbs_code", "WbsCode"}, {"cost_type", "CostType"}, {"budget_amount", "BudgetAmount"}, {"original_budget", "OriginalBudget"}, {"committed_amount", "CommittedAmount"}, {"actual_amount", "ActualAmount"}, {"remaining_amount", "RemainingAmount"}, {"utilization_pct", "UtilizationPct"}, {"variance_amount", "VarianceAmount"}, {"variance_pct", "VariancePct"}, {"budget_status", "BudgetStatus"}, {"period_start", "PeriodStart"}, {"period_end", "PeriodEnd"}, {"posting_date", "PostingDate"}})
in
    Renamed