let
    Source = Databricks.Catalogs("dbc-760a206e-c226.cloud.databricks.com", "/sql/1.0/warehouses/7655fa24e271f9d1", [Catalog="workspace", Database="procurement_gold"]),
    Nav1 = Source{[Name="workspace"]}[Data],
    Nav2 = Nav1{[Name="procurement_gold"]}[Data],
    RawTable = Nav2{[Name="fact_inventory"]}[Data],
    RemoveInternal = Table.RemoveColumns(RawTable, {"_gold_timestamp"}),
    Renamed = Table.RenameColumns(RemoveInternal, {{"movement_id", "MovementId"}, {"material_id", "MaterialId"}, {"grn_id", "GrnId"}, {"movement_type", "MovementType"}, {"movement_date", "MovementDate"}, {"quantity", "Quantity"}, {"unit_cost", "UnitCost"}, {"total_value", "TotalValue"}, {"from_location", "FromLocation"}, {"to_location", "ToLocation"}, {"batch_number", "BatchNumber"}, {"unit_of_measure", "UnitOfMeasure"}, {"reason_code", "ReasonCode"}, {"posted_by", "PostedBy"}})
in
    Renamed