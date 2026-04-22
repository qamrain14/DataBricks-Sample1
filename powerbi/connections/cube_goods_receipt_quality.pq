let
    Source = Databricks.Catalogs("dbc-760a206e-c226.cloud.databricks.com", "/sql/1.0/warehouses/7655fa24e271f9d1", [Catalog="workspace", Database="procurement_semantic"]),
    Nav1 = Source{[Name="workspace"]}[Data],
    Nav2 = Nav1{[Name="procurement_semantic"]}[Data],
    RawTable = Nav2{[Name="cube_goods_receipt_quality"]}[Data],
    Renamed = Table.RenameColumns(RawTable, {{"vendor_id", "VendorId"}, {"vendor_name", "VendorName"}, {"vendor_sector", "VendorSector"}, {"material_id", "MaterialId"}, {"material_name", "MaterialName"}, {"material_category", "MaterialCategory"}, {"receipt_count", "ReceiptCount"}, {"total_qty_received", "TotalQtyReceived"}, {"total_qty_accepted", "TotalQtyAccepted"}, {"total_qty_rejected", "TotalQtyRejected"}, {"avg_acceptance_rate", "AvgAcceptanceRate"}, {"min_acceptance_rate", "MinAcceptanceRate"}, {"total_receipt_value", "TotalReceiptValue"}, {"defect_rate_pct", "DefectRatePct"}, {"quality_tier", "QualityTier"}, {"estimated_rejection_value", "EstimatedRejectionValue"}})
in
    Renamed