let
    Source = Databricks.Catalogs("dbc-760a206e-c226.cloud.databricks.com", "/sql/1.0/warehouses/7655fa24e271f9d1", [Catalog="workspace", Database="procurement_semantic"]),
    Nav1 = Source{[Name="workspace"]}[Data],
    Nav2 = Nav1{[Name="procurement_semantic"]}[Data],
    RawTable = Nav2{[Name="cube_sales_analysis"]}[Data],
    Renamed = Table.RenameColumns(RawTable, {{"product_name", "ProductName"}, {"customer_name", "CustomerName"}, {"customer_type", "CustomerType"}, {"order_type", "OrderType"}, {"year", "Year"}, {"quarter", "Quarter"}, {"year_month", "YearMonth"}, {"sales_count", "SalesCount"}, {"total_quantity", "TotalQuantity"}, {"total_revenue", "TotalRevenue"}, {"total_cogs", "TotalCogs"}, {"total_margin", "TotalMargin"}, {"margin_pct", "MarginPct"}, {"avg_unit_price", "AvgUnitPrice"}, {"prev_year_revenue", "PrevYearRevenue"}, {"revenue_yoy_growth_pct", "RevenueYoyGrowthPct"}})
in
    Renamed