let
    Source = Databricks.Catalogs("dbc-760a206e-c226.cloud.databricks.com", "/sql/1.0/warehouses/7655fa24e271f9d1", [Catalog="workspace", Database="procurement_semantic"]),
    Nav1 = Source{[Name="workspace"]}[Data],
    Nav2 = Nav1{[Name="procurement_semantic"]}[Data],
    RawTable = Nav2{[Name="cube_vendor_risk"]}[Data],
    Renamed = Table.RenameColumns(RawTable, {{"vendor_id", "VendorId"}, {"vendor_name", "VendorName"}, {"vendor_sector", "VendorSector"}, {"vendor_country", "VendorCountry"}, {"vendor_city", "VendorCity"}, {"tier", "Tier"}, {"vendor_status", "VendorStatus"}, {"avg_overall_score", "AvgOverallScore"}, {"avg_delivery_score", "AvgDeliveryScore"}, {"avg_quality_score", "AvgQualityScore"}, {"avg_commercial_score", "AvgCommercialScore"}, {"avg_hse_score", "AvgHseScore"}, {"min_overall_score", "MinOverallScore"}, {"evaluation_count", "EvaluationCount"}, {"invoice_count", "InvoiceCount"}, {"overdue_invoices", "OverdueInvoices"}, {"avg_days_outstanding", "AvgDaysOutstanding"}, {"total_invoiced", "TotalInvoiced"}, {"overdue_rate_pct", "OverdueRatePct"}, {"avg_acceptance_rate", "AvgAcceptanceRate"}, {"total_rejected", "TotalRejected"}, {"composite_risk_score", "CompositeRiskScore"}, {"risk_tier", "RiskTier"}, {"risk_rank", "RiskRank"}})
in
    Renamed