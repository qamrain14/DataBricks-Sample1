let
    Source = Databricks.Catalogs("dbc-760a206e-c226.cloud.databricks.com", "/sql/1.0/warehouses/7655fa24e271f9d1", [Catalog="workspace", Database="procurement_gold"]),
    Nav1 = Source{[Name="workspace"]}[Data],
    Nav2 = Nav1{[Name="procurement_gold"]}[Data],
    RawTable = Nav2{[Name="dim_project"]}[Data],
    RemoveInternal = Table.RemoveColumns(RawTable, {"_is_current", "_gold_timestamp"}),
    Renamed = Table.RenameColumns(RemoveInternal, {{"project_id", "ProjectId"}, {"project_code", "ProjectCode"}, {"project_name", "ProjectName"}, {"project_type", "ProjectType"}, {"client_name", "ClientName"}, {"sector", "Sector"}, {"state", "State"}, {"project_manager", "ProjectManager"}, {"contract_value_usd", "ContractValueUsd"}, {"approved_budget_usd", "ApprovedBudgetUsd"}, {"project_status", "ProjectStatus"}, {"status", "Status"}, {"start_date", "StartDate"}, {"end_date", "EndDate"}, {"actual_completion_date", "ActualCompletionDate"}, {"project_duration_days", "ProjectDurationDays"}, {"priority", "Priority"}, {"risk_level", "RiskLevel"}, {"region", "Region"}})
in
    Renamed