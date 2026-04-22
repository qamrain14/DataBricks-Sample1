let
    Source = Databricks.Catalogs("dbc-760a206e-c226.cloud.databricks.com", "/sql/1.0/warehouses/7655fa24e271f9d1", [Catalog="workspace", Database="procurement_gold"]),
    Nav1 = Source{[Name="workspace"]}[Data],
    Nav2 = Nav1{[Name="procurement_gold"]}[Data],
    RawTable = Nav2{[Name="fact_goods_receipts"]}[Data],
    RemoveInternal = Table.RemoveColumns(RawTable, {"_gold_timestamp"}),
    Renamed = Table.RenameColumns(RemoveInternal, {{"grn_id", "GrnId"}, {"po_id", "PoId"}, {"line_item_id", "LineItemId"}, {"vendor_id", "VendorId"}, {"material_id", "MaterialId"}, {"project_id", "ProjectId"}, {"receipt_date", "ReceiptDate"}, {"quantity_received", "QuantityReceived"}, {"quantity_accepted", "QuantityAccepted"}, {"quantity_rejected", "QuantityRejected"}, {"rejection_rate", "RejectionRate"}, {"acceptance_rate", "AcceptanceRate"}, {"receipt_value", "ReceiptValue"}, {"grn_status", "GrnStatus"}, {"storage_location", "StorageLocation"}, {"inspector", "Inspector"}})
in
    Renamed