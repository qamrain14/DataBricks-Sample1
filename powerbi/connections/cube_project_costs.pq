let
    Source = Databricks.Catalogs("dbc-760a206e-c226.cloud.databricks.com", "/sql/1.0/warehouses/7655fa24e271f9d1", [Catalog="workspace", Database="procurement_semantic"]),
    Nav1 = Source{[Name="workspace"]}[Data],
    Nav2 = Nav1{[Name="procurement_semantic"]}[Data],
    RawTable = Nav2{[Name="cube_project_costs"]}[Data],
    Renamed = Table.RenameColumns(RawTable, {{"project_id", "ProjectId"}, {"project_name", "ProjectName"}, {"project_status", "ProjectStatus"}, {"sector", "Sector"}, {"region", "Region"}, {"priority", "Priority"}, {"project_duration_days", "ProjectDurationDays"}, {"total_budget", "TotalBudget"}, {"total_actual", "TotalActual"}, {"total_committed", "TotalCommitted"}, {"total_exposure", "TotalExposure"}, {"total_variance", "TotalVariance"}, {"avg_variance_pct", "AvgVariancePct"}, {"cpi", "Cpi"}, {"is_over_budget", "IsOverBudget"}, {"budget_utilisation_pct", "BudgetUtilisationPct"}, {"wbs_count", "WbsCount"}, {"cost_type_count", "CostTypeCount"}})
in
    Renamed