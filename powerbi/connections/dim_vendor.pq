let
    Source = Databricks.Catalogs("dbc-760a206e-c226.cloud.databricks.com", "/sql/1.0/warehouses/7655fa24e271f9d1", [Catalog="workspace", Database="procurement_gold"]),
    Nav1 = Source{[Name="workspace"]}[Data],
    Nav2 = Nav1{[Name="procurement_gold"]}[Data],
    RawTable = Nav2{[Name="dim_vendor"]}[Data],
    RemoveInternal = Table.RemoveColumns(RawTable, {"_is_current", "_gold_timestamp"}),
    Renamed = Table.RenameColumns(RemoveInternal, {{"vendor_id", "VendorId"}, {"vendor_code", "VendorCode"}, {"vendor_name", "VendorName"}, {"vendor_type", "VendorType"}, {"vendor_sector", "VendorSector"}, {"sector", "Sector"}, {"vendor_country", "VendorCountry"}, {"vendor_state", "VendorState"}, {"vendor_city", "VendorCity"}, {"payment_terms_days", "PaymentTermsDays"}, {"credit_limit_usd", "CreditLimitUsd"}, {"performance_rating", "PerformanceRating"}, {"risk_category", "RiskCategory"}, {"prequalification_status", "PrequalificationStatus"}, {"prequalification_expiry", "PrequalificationExpiry"}, {"iso_certified", "IsoCertified"}, {"hse_rating", "HseRating"}, {"minority_owned", "MinorityOwned"}, {"small_business", "SmallBusiness"}, {"primary_contact", "PrimaryContact"}, {"contact_email", "ContactEmail"}, {"contact_phone", "ContactPhone"}, {"annual_turnover_usd", "AnnualTurnoverUsd"}, {"years_in_business", "YearsInBusiness"}, {"tier", "Tier"}, {"status", "Status"}})
in
    Renamed