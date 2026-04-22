let
    Source = Databricks.Catalogs("dbc-760a206e-c226.cloud.databricks.com", "/sql/1.0/warehouses/7655fa24e271f9d1", [Catalog="workspace", Database="procurement_semantic"]),
    Nav1 = Source{[Name="workspace"]}[Data],
    Nav2 = Nav1{[Name="procurement_semantic"]}[Data],
    RawTable = Nav2{[Name="cube_vendor_performance"]}[Data],
    Renamed = Table.RenameColumns(RawTable, {{"vendor_id", "VendorId"}, {"vendor_name", "VendorName"}, {"sector", "Sector"}, {"tier", "Tier"}, {"country", "Country"}, {"evaluation_period", "EvaluationPeriod"}, {"avg_delivery_score", "AvgDeliveryScore"}, {"avg_quality_score", "AvgQualityScore"}, {"avg_commercial_score", "AvgCommercialScore"}, {"avg_hse_score", "AvgHseScore"}, {"avg_overall_score", "AvgOverallScore"}, {"latest_recommendation", "LatestRecommendation"}, {"evaluation_count", "EvaluationCount"}, {"vendor_rank", "VendorRank"}, {"score_trend", "ScoreTrend"}})
in
    Renamed