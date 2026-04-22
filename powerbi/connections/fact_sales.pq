let
    Source = Databricks.Catalogs("dbc-760a206e-c226.cloud.databricks.com", "/sql/1.0/warehouses/7655fa24e271f9d1", [Catalog="workspace", Database="procurement_gold"]),
    Nav1 = Source{[Name="workspace"]}[Data],
    Nav2 = Nav1{[Name="procurement_gold"]}[Data],
    RawTable = Nav2{[Name="fact_sales"]}[Data],
    Renamed = Table.RenameColumns(RawTable, {{"sales_order_id", "SalesOrderId"}, {"product_name", "ProductName"}, {"customer_name", "CustomerName"}, {"customer_id", "CustomerId"}, {"customer_type", "CustomerType"}, {"order_type", "OrderType"}, {"order_status", "OrderStatus"}, {"payment_status", "PaymentStatus"}, {"order_date", "OrderDate"}, {"delivery_date", "DeliveryDate"}, {"quantity", "Quantity"}, {"unit_price", "UnitPrice"}, {"revenue", "Revenue"}, {"cogs", "Cogs"}, {"gross_margin", "GrossMargin"}, {"margin_pct", "MarginPct"}, {"tax_amount", "TaxAmount"}, {"total_amount", "TotalAmount"}, {"currency", "Currency"}, {"delivery_terms", "DeliveryTerms"}, {"load_timestamp", "LoadTimestamp"}})
in
    Renamed