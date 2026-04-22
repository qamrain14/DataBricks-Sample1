let
    Source = Databricks.Catalogs("dbc-760a206e-c226.cloud.databricks.com", "/sql/1.0/warehouses/7655fa24e271f9d1", [Catalog="workspace", Database="procurement_semantic"]),
    Nav1 = Source{[Name="workspace"]}[Data],
    Nav2 = Nav1{[Name="procurement_semantic"]}[Data],
    RawTable = Nav2{[Name="cube_project_timeline"]}[Data],
    Renamed = Table.RenameColumns(RawTable, {{"project_id", "ProjectId"}, {"project_name", "ProjectName"}, {"project_status", "ProjectStatus"}, {"sector", "Sector"}, {"region", "Region"}, {"priority", "Priority"}, {"start_date", "StartDate"}, {"end_date", "EndDate"}, {"planned_duration_days", "PlannedDurationDays"}, {"first_spend_date", "FirstSpendDate"}, {"last_spend_date", "LastSpendDate"}, {"actual_active_days", "ActualActiveDays"}, {"schedule_variance_days", "ScheduleVarianceDays"}, {"schedule_status", "ScheduleStatus"}, {"planned_pct_complete", "PlannedPctComplete"}, {"cost_pct_complete", "CostPctComplete"}, {"total_actual", "TotalActual"}, {"total_budget", "TotalBudget"}, {"active_wbs_count", "ActiveWbsCount"}, {"contract_count", "ContractCount"}, {"active_contracts", "ActiveContracts"}, {"completed_contracts", "CompletedContracts"}, {"total_contract_value", "TotalContractValue"}})
in
    Renamed