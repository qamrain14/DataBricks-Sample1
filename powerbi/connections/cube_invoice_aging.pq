let
    Source = Databricks.Catalogs("dbc-760a206e-c226.cloud.databricks.com", "/sql/1.0/warehouses/7655fa24e271f9d1", [Catalog="workspace", Database="procurement_semantic"]),
    Nav1 = Source{[Name="workspace"]}[Data],
    Nav2 = Nav1{[Name="procurement_semantic"]}[Data],
    RawTable = Nav2{[Name="cube_invoice_aging"]}[Data],
    Renamed = Table.RenameColumns(RawTable, {{"vendor_id", "VendorId"}, {"vendor_name", "VendorName"}, {"vendor_sector", "VendorSector"}, {"project_id", "ProjectId"}, {"project_name", "ProjectName"}, {"year", "Year"}, {"quarter", "Quarter"}, {"year_month", "YearMonth"}, {"aging_bucket", "AgingBucket"}, {"invoice_count", "InvoiceCount"}, {"total_net_amount", "TotalNetAmount"}, {"total_gross_amount", "TotalGrossAmount"}, {"total_tax_amount", "TotalTaxAmount"}, {"avg_days_outstanding", "AvgDaysOutstanding"}, {"max_days_outstanding", "MaxDaysOutstanding"}, {"overdue_amount", "OverdueAmount"}, {"overdue_count", "OverdueCount"}, {"overdue_pct", "OverduePct"}})
in
    Renamed