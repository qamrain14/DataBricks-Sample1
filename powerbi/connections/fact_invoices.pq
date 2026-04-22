let
    Source = Databricks.Catalogs("dbc-760a206e-c226.cloud.databricks.com", "/sql/1.0/warehouses/7655fa24e271f9d1", [Catalog="workspace", Database="procurement_gold"]),
    Nav1 = Source{[Name="workspace"]}[Data],
    Nav2 = Nav1{[Name="procurement_gold"]}[Data],
    RawTable = Nav2{[Name="fact_invoices"]}[Data],
    RemoveInternal = Table.RemoveColumns(RawTable, {"_gold_timestamp"}),
    Renamed = Table.RenameColumns(RemoveInternal, {{"invoice_id", "InvoiceId"}, {"vendor_id", "VendorId"}, {"po_id", "PoId"}, {"project_id", "ProjectId"}, {"contract_id", "ContractId"}, {"invoice_number", "InvoiceNumber"}, {"invoice_type", "InvoiceType"}, {"payment_status", "PaymentStatus"}, {"invoice_date", "InvoiceDate"}, {"due_date", "DueDate"}, {"payment_date", "PaymentDate"}, {"invoice_amount", "InvoiceAmount"}, {"net_amount", "NetAmount"}, {"tax_rate", "TaxRate"}, {"tax_amount", "TaxAmount"}, {"total_amount", "TotalAmount"}, {"gross_amount", "GrossAmount"}, {"paid_amount", "PaidAmount"}, {"retention_amount", "RetentionAmount"}, {"days_to_pay", "DaysToPay"}, {"days_overdue", "DaysOverdue"}, {"days_outstanding", "DaysOutstanding"}, {"aging_bucket", "AgingBucket"}, {"currency", "Currency"}, {"approved_by", "ApprovedBy"}, {"notes", "Notes"}})
in
    Renamed