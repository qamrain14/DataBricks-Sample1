let
    Source = Databricks.Catalogs("dbc-760a206e-c226.cloud.databricks.com", "/sql/1.0/warehouses/7655fa24e271f9d1", [Catalog="workspace", Database="procurement_gold"]),
    Nav1 = Source{[Name="workspace"]}[Data],
    Nav2 = Nav1{[Name="procurement_gold"]}[Data],
    RawTable = Nav2{[Name="fact_purchase_orders"]}[Data],
    RemoveInternal = Table.RemoveColumns(RawTable, {"_gold_timestamp"}),
    Renamed = Table.RenameColumns(RemoveInternal, {{"po_id", "PoId"}, {"vendor_id", "VendorId"}, {"project_id", "ProjectId"}, {"po_date", "PoDate"}, {"delivery_date", "DeliveryDate"}, {"expected_delivery_date", "ExpectedDeliveryDate"}, {"approval_date", "ApprovalDate"}, {"po_value", "PoValue"}, {"tax_amount", "TaxAmount"}, {"total_value", "TotalValue"}, {"total_amount", "TotalAmount"}, {"po_status", "PoStatus"}, {"po_type", "PoType"}, {"priority", "Priority"}, {"currency", "Currency"}, {"payment_terms", "PaymentTerms"}, {"line_total", "LineTotal"}, {"total_quantity", "TotalQuantity"}})
in
    Renamed