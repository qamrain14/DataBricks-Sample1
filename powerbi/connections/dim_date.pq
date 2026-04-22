let
    Source = Databricks.Catalogs("dbc-760a206e-c226.cloud.databricks.com", "/sql/1.0/warehouses/7655fa24e271f9d1", [Catalog="workspace", Database="procurement_gold"]),
    Nav1 = Source{[Name="workspace"]}[Data],
    Nav2 = Nav1{[Name="procurement_gold"]}[Data],
    RawTable = Nav2{[Name="dim_date"]}[Data],
    Renamed = Table.RenameColumns(RawTable, {{"date_key", "DateKey"}, {"date", "Date"}, {"year", "Year"}, {"quarter", "Quarter"}, {"month", "Month"}, {"day", "Day"}, {"day_of_week", "DayOfWeek"}, {"day_name", "DayName"}, {"month_name", "MonthName"}, {"year_month", "YearMonth"}, {"fiscal_period", "FiscalPeriod"}, {"fiscal_year", "FiscalYear"}, {"fiscal_quarter", "FiscalQuarter"}})
in
    Renamed