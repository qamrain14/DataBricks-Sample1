let
    Source = Databricks.Catalogs("dbc-760a206e-c226.cloud.databricks.com", "/sql/1.0/warehouses/7655fa24e271f9d1", [Catalog="workspace", Database="procurement_gold"]),
    Nav1 = Source{[Name="workspace"]}[Data],
    Nav2 = Nav1{[Name="procurement_gold"]}[Data],
    RawTable = Nav2{[Name="dim_contract"]}[Data],
    RemoveInternal = Table.RemoveColumns(RawTable, {"_is_current", "_gold_timestamp"}),
    Renamed = Table.RenameColumns(RemoveInternal, {{"contract_id", "ContractId"}, {"vendor_id", "VendorId"}, {"project_id", "ProjectId"}, {"contract_type", "ContractType"}, {"contract_status", "ContractStatus"}, {"status", "Status"}, {"contract_value", "ContractValue"}, {"original_value", "OriginalValue"}, {"revised_value", "RevisedValue"}, {"start_date", "StartDate"}, {"end_date", "EndDate"}, {"contract_duration_days", "ContractDurationDays"}, {"payment_terms", "PaymentTerms"}, {"retention_pct", "RetentionPct"}, {"performance_bond", "PerformanceBond"}, {"change_order_count", "ChangeOrderCount"}, {"change_order_value", "ChangeOrderValue"}, {"cost_growth_pct", "CostGrowthPct"}, {"approved_by", "ApprovedBy"}})
in
    Renamed