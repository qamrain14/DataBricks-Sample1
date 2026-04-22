let
    Source = Databricks.Catalogs("dbc-760a206e-c226.cloud.databricks.com", "/sql/1.0/warehouses/7655fa24e271f9d1", [Catalog="workspace", Database="procurement_semantic"]),
    Nav1 = Source{[Name="workspace"]}[Data],
    Nav2 = Nav1{[Name="procurement_semantic"]}[Data],
    RawTable = Nav2{[Name="cube_spend_by_sector"]}[Data],
    Renamed = Table.RenameColumns(RawTable, {{"sector_key", "SectorKey"}, {"sector_name", "SectorName"}, {"vendor_id", "VendorId"}, {"vendor_name", "VendorName"}, {"country", "Country"}, {"year", "Year"}, {"quarter", "Quarter"}, {"po_count", "PoCount"}, {"total_spend", "TotalSpend"}, {"total_quantity", "TotalQuantity"}, {"avg_po_value", "AvgPoValue"}, {"vendor_sector_share_pct", "VendorSectorSharePct"}, {"sector_spend_rank", "SectorSpendRank"}, {"prev_year_spend", "PrevYearSpend"}, {"yoy_growth_pct", "YoyGrowthPct"}})
in
    Renamed