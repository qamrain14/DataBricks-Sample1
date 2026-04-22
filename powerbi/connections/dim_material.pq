let
    Source = Databricks.Catalogs("dbc-760a206e-c226.cloud.databricks.com", "/sql/1.0/warehouses/7655fa24e271f9d1", [Catalog="workspace", Database="procurement_gold"]),
    Nav1 = Source{[Name="workspace"]}[Data],
    Nav2 = Nav1{[Name="procurement_gold"]}[Data],
    RawTable = Nav2{[Name="dim_material"]}[Data],
    RemoveInternal = Table.RemoveColumns(RawTable, {"_is_current", "_gold_timestamp"}),
    Renamed = Table.RenameColumns(RemoveInternal, {{"material_id", "MaterialId"}, {"material_code", "MaterialCode"}, {"material_name", "MaterialName"}, {"category", "Category"}, {"material_category", "MaterialCategory"}, {"sub_category", "SubCategory"}, {"unit_price", "UnitPrice"}, {"weight_kg", "WeightKg"}, {"lead_time_days", "LeadTimeDays"}, {"is_active", "IsActive"}})
in
    Renamed