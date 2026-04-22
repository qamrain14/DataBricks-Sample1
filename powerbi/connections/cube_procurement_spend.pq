let
    Source = Databricks.Catalogs("dbc-760a206e-c226.cloud.databricks.com", "/sql/1.0/warehouses/7655fa24e271f9d1", [Catalog="workspace", Database="procurement_semantic"]),
    Nav1 = Source{[Name="workspace"]}[Data],
    Nav2 = Nav1{[Name="procurement_semantic"]}[Data],
    RawTable = Nav2{[Name="cube_procurement_spend"]}[Data],
    Renamed = Table.RenameColumns(RawTable, {{"vendor_id", "VendorId"}, {"vendor_name", "VendorName"}, {"vendor_sector", "VendorSector"}, {"project_id", "ProjectId"}, {"project_name", "ProjectName"}, {"year", "Year"}, {"quarter", "Quarter"}, {"year_month", "YearMonth"}, {"po_count", "PoCount"}, {"total_spend", "TotalSpend"}, {"total_line_value", "TotalLineValue"}, {"total_quantity", "TotalQuantity"}, {"avg_po_value", "AvgPoValue"}, {"prev_year_spend", "PrevYearSpend"}, {"yoy_growth_pct", "YoyGrowthPct"}})
in
    Renamed