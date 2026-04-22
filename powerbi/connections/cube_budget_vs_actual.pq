let
    Source = Databricks.Catalogs("dbc-760a206e-c226.cloud.databricks.com", "/sql/1.0/warehouses/7655fa24e271f9d1", [Catalog="workspace", Database="procurement_gold"]),
    Nav1 = Source{[Name="workspace"]}[Data],
    Nav2 = Nav1{[Name="procurement_gold"]}[Data],
    RawTable = Nav2{[Name="cube_budget_vs_actual"]}[Data],
    Renamed = Table.RenameColumns(RawTable, {{"project_id", "ProjectId"}, {"project_name", "ProjectName"}, {"sector", "Sector"}, {"region", "Region"}, {"wbs_element", "WbsElement"}, {"cost_type", "CostType"}, {"budget_amount", "BudgetAmount"}, {"actual_amount", "ActualAmount"}, {"committed_amount", "CommittedAmount"}, {"total_exposure", "TotalExposure"}, {"variance_amount", "VarianceAmount"}, {"avg_variance_pct", "AvgVariancePct"}, {"variance_class", "VarianceClass"}, {"cpi", "Cpi"}, {"budget_consumed_pct", "BudgetConsumedPct"}})
in
    Renamed