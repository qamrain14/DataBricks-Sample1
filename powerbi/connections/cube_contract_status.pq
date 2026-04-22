let
    Source = Databricks.Catalogs("dbc-760a206e-c226.cloud.databricks.com", "/sql/1.0/warehouses/7655fa24e271f9d1", [Catalog="workspace", Database="procurement_semantic"]),
    Nav1 = Source{[Name="workspace"]}[Data],
    Nav2 = Nav1{[Name="procurement_semantic"]}[Data],
    RawTable = Nav2{[Name="cube_contract_status"]}[Data],
    Renamed = Table.RenameColumns(RawTable, {{"contract_id", "ContractId"}, {"contract_type", "ContractType"}, {"status", "Status"}, {"vendor_id", "VendorId"}, {"vendor_name", "VendorName"}, {"project_id", "ProjectId"}, {"project_name", "ProjectName"}, {"start_date", "StartDate"}, {"end_date", "EndDate"}, {"original_value", "OriginalValue"}, {"revised_value", "RevisedValue"}, {"items_total", "ItemsTotal"}, {"item_count", "ItemCount"}, {"contract_duration_days", "ContractDurationDays"}, {"cost_growth_pct", "CostGrowthPct"}, {"value_utilisation_pct", "ValueUtilisationPct"}, {"cost_risk_flag", "CostRiskFlag"}, {"schedule_risk_flag", "ScheduleRiskFlag"}, {"remaining_value", "RemainingValue"}})
in
    Renamed