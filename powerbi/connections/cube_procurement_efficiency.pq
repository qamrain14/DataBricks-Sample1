let
    Source = Databricks.Catalogs("dbc-760a206e-c226.cloud.databricks.com", "/sql/1.0/warehouses/7655fa24e271f9d1", [Catalog="workspace", Database="procurement_semantic"]),
    Nav1 = Source{[Name="workspace"]}[Data],
    Nav2 = Nav1{[Name="procurement_semantic"]}[Data],
    RawTable = Nav2{[Name="cube_procurement_efficiency"]}[Data],
    Renamed = Table.RenameColumns(RawTable, {{"vendor_id", "VendorId"}, {"vendor_name", "VendorName"}, {"vendor_sector", "VendorSector"}, {"year", "Year"}, {"quarter", "Quarter"}, {"year_month", "YearMonth"}, {"po_count", "PoCount"}, {"avg_order_to_receipt_days", "AvgOrderToReceiptDays"}, {"median_order_to_receipt", "MedianOrderToReceipt"}, {"avg_receipt_to_invoice_days", "AvgReceiptToInvoiceDays"}, {"median_receipt_to_invoice", "MedianReceiptToInvoice"}, {"avg_invoice_to_payment_days", "AvgInvoiceToPaymentDays"}, {"median_invoice_to_payment", "MedianInvoiceToPayment"}, {"avg_total_p2p_days", "AvgTotalP2pDays"}, {"median_total_p2p", "MedianTotalP2p"}, {"min_p2p_days", "MinP2pDays"}, {"max_p2p_days", "MaxP2pDays"}, {"total_spend", "TotalSpend"}, {"prev_quarter_avg_p2p_days", "PrevQuarterAvgP2pDays"}, {"complete_p2p_pct", "CompleteP2pPct"}})
in
    Renamed