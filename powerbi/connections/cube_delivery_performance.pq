let
    Source = Databricks.Catalogs("dbc-760a206e-c226.cloud.databricks.com", "/sql/1.0/warehouses/7655fa24e271f9d1", [Catalog="workspace", Database="procurement_semantic"]),
    Nav1 = Source{[Name="workspace"]}[Data],
    Nav2 = Nav1{[Name="procurement_semantic"]}[Data],
    RawTable = Nav2{[Name="cube_delivery_performance"]}[Data],
    Renamed = Table.RenameColumns(RawTable, {{"vendor_id", "VendorId"}, {"vendor_name", "VendorName"}, {"vendor_sector", "VendorSector"}, {"year", "Year"}, {"quarter", "Quarter"}, {"year_month", "YearMonth"}, {"delivery_count", "DeliveryCount"}, {"avg_variance_days", "AvgVarianceDays"}, {"max_variance_days", "MaxVarianceDays"}, {"min_variance_days", "MinVarianceDays"}, {"on_time_rate_pct", "OnTimeRatePct"}, {"on_time_count", "OnTimeCount"}, {"late_1wk_count", "Late1wkCount"}, {"late_1mo_count", "Late1moCount"}, {"late_over_mo_count", "LateOverMoCount"}, {"total_value_delivered", "TotalValueDelivered"}, {"prev_quarter_on_time_pct", "PrevQuarterOnTimePct"}})
in
    Renamed