let
    Source = Databricks.Catalogs("dbc-760a206e-c226.cloud.databricks.com", "/sql/1.0/warehouses/7655fa24e271f9d1", [Catalog="workspace", Database="procurement_gold"]),
    Nav1 = Source{[Name="workspace"]}[Data],
    Nav2 = Nav1{[Name="procurement_gold"]}[Data],
    RawTable = Nav2{[Name="fact_contracts"]}[Data],
    RemoveInternal = Table.RemoveColumns(RawTable, {"_gold_timestamp"}),
    Renamed = Table.RenameColumns(RemoveInternal, {{"contract_id", "ContractId"}, {"vendor_id", "VendorId"}, {"project_id", "ProjectId"}, {"contract_type", "ContractType"}, {"contract_status", "ContractStatus"}, {"contract_value", "ContractValue"}, {"revised_value", "RevisedValue"}, {"start_date", "StartDate"}, {"change_order_count", "ChangeOrderCount"}, {"change_order_value", "ChangeOrderValue"}, {"items_total", "ItemsTotal"}, {"item_count", "ItemCount"}, {"value_utilisation_pct", "ValueUtilisationPct"}, {"payment_terms", "PaymentTerms"}, {"retention_pct", "RetentionPct"}, {"penalty_clause", "PenaltyClause"}, {"performance_bond", "PerformanceBond"}, {"currency", "Currency"}})
in
    Renamed