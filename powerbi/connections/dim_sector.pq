let
    Source = Databricks.Catalogs("dbc-760a206e-c226.cloud.databricks.com", "/sql/1.0/warehouses/7655fa24e271f9d1", [Catalog="workspace", Database="procurement_gold"]),
    Nav1 = Source{[Name="workspace"]}[Data],
    Nav2 = Nav1{[Name="procurement_gold"]}[Data],
    RawTable = Nav2{[Name="dim_sector"]}[Data],
    RemoveInternal = Table.RemoveColumns(RawTable, {"_is_current", "_gold_timestamp"}),
    Renamed = Table.RenameColumns(RemoveInternal, {{"sector_key", "SectorKey"}, {"sector_name", "SectorName"}})
in
    Renamed