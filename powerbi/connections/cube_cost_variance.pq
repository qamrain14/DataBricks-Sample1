let
    Source = Databricks.Catalogs("dbc-760a206e-c226.cloud.databricks.com", "/sql/1.0/warehouses/7655fa24e271f9d1", [Catalog="workspace", Database="procurement_semantic"]),
    Nav1 = Source{[Name="workspace"]}[Data],
    Nav2 = Nav1{[Name="procurement_semantic"]}[Data],
    RawTable = Nav2{[Name="cube_cost_variance"]}[Data],
    Renamed = Table.RenameColumns(RawTable, {{"project_id", "ProjectId"}, {"project_name", "ProjectName"}, {"project_status", "ProjectStatus"}, {"sector", "Sector"}, {"region", "Region"}, {"cost_type", "CostType"}, {"wbs_element", "WbsElement"}, {"budget_amount", "BudgetAmount"}, {"actual_amount", "ActualAmount"}, {"committed_amount", "CommittedAmount"}, {"variance_amount", "VarianceAmount"}, {"overrun_amount", "OverrunAmount"}, {"overrun_severity", "OverrunSeverity"}, {"cpi", "Cpi"}, {"remaining_budget", "RemainingBudget"}, {"burn_rate_pct", "BurnRatePct"}, {"overrun_rank", "OverrunRank"}})
in
    Renamed