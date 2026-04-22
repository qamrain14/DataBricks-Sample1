let
    Source = Databricks.Catalogs("dbc-760a206e-c226.cloud.databricks.com", "/sql/1.0/warehouses/7655fa24e271f9d1", [Catalog="workspace", Database="procurement_gold"]),
    Nav1 = Source{[Name="workspace"]}[Data],
    Nav2 = Nav1{[Name="procurement_gold"]}[Data],
    RawTable = Nav2{[Name="fact_vendor_performance"]}[Data],
    RemoveInternal = Table.RemoveColumns(RawTable, {"_gold_timestamp"}),
    Renamed = Table.RenameColumns(RemoveInternal, {{"performance_id", "PerformanceId"}, {"vendor_id", "VendorId"}, {"project_id", "ProjectId"}, {"evaluation_date", "EvaluationDate"}, {"evaluation_period", "EvaluationPeriod"}, {"delivery_score", "DeliveryScore"}, {"quality_score", "QualityScore"}, {"commercial_score", "CommercialScore"}, {"hse_score", "HseScore"}, {"compliance_score", "ComplianceScore"}, {"overall_score", "OverallScore"}, {"recommendation", "Recommendation"}, {"evaluator_id", "EvaluatorId"}, {"notes", "Notes"}})
in
    Renamed