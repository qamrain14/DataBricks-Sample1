let
    Source = Databricks.Catalogs("dbc-760a206e-c226.cloud.databricks.com", "/sql/1.0/warehouses/7655fa24e271f9d1", [Catalog="workspace", Database="procurement_semantic"]),
    Nav1 = Source{[Name="workspace"]}[Data],
    Nav2 = Nav1{[Name="procurement_semantic"]}[Data],
    RawTable = Nav2{[Name="cube_inventory_status"]}[Data],
    Renamed = Table.RenameColumns(RawTable, {{"material_id", "MaterialId"}, {"material_name", "MaterialName"}, {"material_category", "MaterialCategory"}, {"year", "Year"}, {"quarter", "Quarter"}, {"year_month", "YearMonth"}, {"receipt_count", "ReceiptCount"}, {"issue_count", "IssueCount"}, {"return_count", "ReturnCount"}, {"total_movement_count", "TotalMovementCount"}, {"qty_received", "QtyReceived"}, {"qty_issued", "QtyIssued"}, {"qty_returned", "QtyReturned"}, {"net_quantity_flow", "NetQuantityFlow"}, {"receipt_value", "ReceiptValue"}, {"issue_value", "IssueValue"}, {"return_value", "ReturnValue"}, {"total_movement_value", "TotalMovementValue"}, {"avg_unit_cost", "AvgUnitCost"}, {"issue_to_receipt_ratio", "IssueToReceiptRatio"}})
in
    Renamed