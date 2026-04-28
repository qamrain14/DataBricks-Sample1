# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer &ndash; Star Schema (Dimensional Model)
# MAGIC Builds **dimensions** and **fact tables** from the Silver layer.
# MAGIC | Layer | Tables |
# MAGIC |-------|--------|
# MAGIC | Dimensions | dim_date, dim_vendor, dim_project, dim_material, dim_employee, dim_geography, dim_contract, dim_cost_center, dim_sector |
# MAGIC | Facts | fact_purchase_orders, fact_invoices, fact_goods_receipts, fact_project_costs, fact_project_actuals, fact_vendor_performance, fact_inventory, fact_contracts, fact_sales |
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC import dlt
# MAGIC from pyspark.sql.functions import (
# MAGIC     col, when, trim, upper, lit, current_timestamp, current_date, to_date,
# MAGIC     year, month, dayofmonth, dayofweek, quarter, date_format,
# MAGIC     concat, lpad, coalesce, round as spark_round, sha2,
# MAGIC     sum as spark_sum, count as spark_count, max as spark_max
# MAGIC )
# MAGIC from pyspark.sql.types import BooleanType
# MAGIC CATALOG = spark.conf.get("catalog", "procurement_lakehouse")
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC %md
# MAGIC ## Dimensions
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC @dlt.table(
# MAGIC     name="dim_date",
# MAGIC     comment="Date dimension - full calendar 2020-2030",
# MAGIC     table_properties={"quality": "gold", "layer": "gold"}
# MAGIC )
# MAGIC def gold_dim_date():
# MAGIC     df = spark.sql(
# MAGIC         "SELECT explode(sequence("
# MAGIC         "to_date('2020-01-01'), to_date('2030-12-31'), interval 1 day"
# MAGIC         ")) AS full_date"
# MAGIC     )
# MAGIC     return df.select(
# MAGIC         date_format("full_date", "yyyyMMdd").cast("int").alias("date_key"),
# MAGIC         col("full_date").alias("date"),
# MAGIC         year("full_date").alias("year"),
# MAGIC         quarter("full_date").alias("quarter"),
# MAGIC         month("full_date").alias("month"),
# MAGIC         dayofmonth("full_date").alias("day"),
# MAGIC         dayofweek("full_date").alias("day_of_week"),
# MAGIC         date_format("full_date", "EEEE").alias("day_name"),
# MAGIC         date_format("full_date", "MMMM").alias("month_name"),
# MAGIC         concat(
# MAGIC             year("full_date").cast("string"), lit("-"),
# MAGIC             lpad(month("full_date").cast("string"), 2, "0")
# MAGIC         ).alias("year_month"),
# MAGIC         when(month("full_date") <= 6,
# MAGIC             concat(lit("FY"), year("full_date").cast("string"), lit("-H1"))
# MAGIC         ).otherwise(
# MAGIC             concat(lit("FY"), year("full_date").cast("string"), lit("-H2"))
# MAGIC         ).alias("fiscal_period"),
# MAGIC         year("full_date").alias("fiscal_year"),
# MAGIC         quarter("full_date").alias("fiscal_quarter")
# MAGIC     )
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC @dlt.table(
# MAGIC     name="dim_vendor",
# MAGIC     comment="Vendor dimension with tier and status derivations",
# MAGIC     table_properties={"quality": "gold", "layer": "gold"}
# MAGIC )
# MAGIC def gold_dim_vendor():
# MAGIC     df = spark.read.table(f"{CATALOG}.procurement_silver.silver_vendors")
# MAGIC     return df.select(
# MAGIC         col("vendor_id"),
# MAGIC         col("vendor_code"),
# MAGIC         col("vendor_name"),
# MAGIC         col("vendor_type"),
# MAGIC         col("sector_specialization").alias("vendor_sector"),
# MAGIC         upper(col("sector_specialization")).alias("sector"),
# MAGIC         col("country").alias("vendor_country"),
# MAGIC         col("state").alias("vendor_state"),
# MAGIC         col("city").alias("vendor_city"),
# MAGIC         col("payment_terms_days"),
# MAGIC         col("credit_limit_usd"),
# MAGIC         col("performance_rating"),
# MAGIC         col("risk_category"),
# MAGIC         col("prequalification_status"),
# MAGIC         col("prequalification_expiry_date").alias("prequalification_expiry"),
# MAGIC         col("iso_certified"),
# MAGIC         col("hse_rating"),
# MAGIC         col("minority_owned"),
# MAGIC         col("small_business"),
# MAGIC         col("contact_name").alias("primary_contact"),
# MAGIC         col("contact_email"),
# MAGIC         col("contact_phone"),
# MAGIC         col("annual_turnover_usd"),
# MAGIC         col("years_in_business"),
# MAGIC         when(col("performance_rating") >= 4, lit("STRATEGIC"))
# MAGIC             .when(col("performance_rating") >= 3, lit("PREFERRED"))
# MAGIC             .otherwise(lit("STANDARD")).alias("tier"),
# MAGIC         when(upper(col("prequalification_status")) == "APPROVED", lit("ACTIVE"))
# MAGIC             .when(upper(col("prequalification_status")) == "PENDING", lit("PENDING"))
# MAGIC             .otherwise(lit("INACTIVE")).alias("status"),
# MAGIC         lit(True).cast(BooleanType()).alias("_is_current"),
# MAGIC         current_timestamp().alias("_gold_timestamp")
# MAGIC     )
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC @dlt.table(
# MAGIC     name="dim_project",
# MAGIC     comment="Project dimension with region derivation",
# MAGIC     table_properties={"quality": "gold", "layer": "gold"}
# MAGIC )
# MAGIC def gold_dim_project():
# MAGIC     df = spark.read.table(f"{CATALOG}.procurement_silver.silver_projects")
# MAGIC     return df.select(
# MAGIC         col("project_id"),
# MAGIC         col("project_code"),
# MAGIC         col("project_name"),
# MAGIC         col("project_type"),
# MAGIC         col("client_name"),
# MAGIC         col("sector"),
# MAGIC         col("state"),
# MAGIC         col("project_manager"),
# MAGIC         col("contract_value_usd"),
# MAGIC         col("approved_budget_usd"),
# MAGIC         col("project_status"),
# MAGIC         col("project_status").alias("status"),
# MAGIC         col("start_date"),
# MAGIC         col("planned_completion_date").alias("end_date"),
# MAGIC         col("actual_completion_date"),
# MAGIC         col("planned_duration_days").alias("project_duration_days"),
# MAGIC         col("priority"),
# MAGIC         col("risk_level"),
# MAGIC         when(col("country").isin("US", "CA", "MX"), lit("NORTH AMERICA"))
# MAGIC             .when(col("country").isin("GB", "DE", "FR", "IT", "ES", "NL", "NO", "SE"), lit("EUROPE"))
# MAGIC             .when(col("country").isin("AE", "SA", "QA", "KW", "OM", "BH"), lit("MIDDLE EAST"))
# MAGIC             .when(col("country").isin("AU", "NZ", "SG", "MY", "IN", "JP", "KR", "CN"), lit("ASIA PACIFIC"))
# MAGIC             .otherwise(lit("OTHER")).alias("region"),
# MAGIC         lit(True).cast(BooleanType()).alias("_is_current"),
# MAGIC         current_timestamp().alias("_gold_timestamp")
# MAGIC     )
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC @dlt.table(
# MAGIC     name="dim_material",
# MAGIC     comment="Material dimension",
# MAGIC     table_properties={"quality": "gold", "layer": "gold"}
# MAGIC )
# MAGIC def gold_dim_material():
# MAGIC     df = spark.read.table(f"{CATALOG}.procurement_silver.silver_materials")
# MAGIC     return df.select(
# MAGIC         col("material_id"),
# MAGIC         col("material_code"),
# MAGIC         col("material_name"),
# MAGIC         col("category"),
# MAGIC         col("category").alias("material_category"),
# MAGIC         col("sub_category"),
# MAGIC         col("unit_price"),
# MAGIC         col("weight_kg"),
# MAGIC         col("lead_time_days"),
# MAGIC         col("is_active"),
# MAGIC         lit(True).cast(BooleanType()).alias("_is_current"),
# MAGIC         current_timestamp().alias("_gold_timestamp")
# MAGIC     )
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC @dlt.table(
# MAGIC     name="dim_employee",
# MAGIC     comment="Employee dimension",
# MAGIC     table_properties={"quality": "gold", "layer": "gold"}
# MAGIC )
# MAGIC def gold_dim_employee():
# MAGIC     df = spark.read.table(f"{CATALOG}.procurement_silver.silver_employees")
# MAGIC     return df.select(
# MAGIC         col("employee_id"),
# MAGIC         col("employee_code"),
# MAGIC         col("full_name"),
# MAGIC         col("department"),
# MAGIC         col("job_title"),
# MAGIC         col("grade"),
# MAGIC         col("approval_limit"),
# MAGIC         col("location"),
# MAGIC         col("is_active"),
# MAGIC         col("cost_center"),
# MAGIC         col("hire_date"),
# MAGIC         lit(True).cast(BooleanType()).alias("_is_current"),
# MAGIC         current_timestamp().alias("_gold_timestamp")
# MAGIC     )
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC @dlt.table(
# MAGIC     name="dim_geography",
# MAGIC     comment="Geography dimension - distinct country/state/city with region",
# MAGIC     table_properties={"quality": "gold", "layer": "gold"}
# MAGIC )
# MAGIC def gold_dim_geography():
# MAGIC     projects = spark.read.table(f"{CATALOG}.procurement_silver.silver_projects").select("country", "state", "city")
# MAGIC     vendors = spark.read.table(f"{CATALOG}.procurement_silver.silver_vendors").select("country", "state", "city")
# MAGIC     geo = projects.union(vendors).distinct()
# MAGIC     return geo.select(
# MAGIC         sha2(concat(col("country"), lit("|"), col("state"), lit("|"), col("city")), 256).alias("geography_key"),
# MAGIC         col("country"),
# MAGIC         col("state"),
# MAGIC         col("city"),
# MAGIC         when(col("country").isin("US", "CA", "MX"), lit("NORTH AMERICA"))
# MAGIC             .when(col("country").isin("GB", "DE", "FR", "IT", "ES", "NL", "NO", "SE"), lit("EUROPE"))
# MAGIC             .when(col("country").isin("AE", "SA", "QA", "KW", "OM", "BH"), lit("MIDDLE EAST"))
# MAGIC             .when(col("country").isin("AU", "NZ", "SG", "MY", "IN", "JP", "KR", "CN"), lit("ASIA PACIFIC"))
# MAGIC             .otherwise(lit("OTHER")).alias("region"),
# MAGIC         lit(True).cast(BooleanType()).alias("_is_current"),
# MAGIC         current_timestamp().alias("_gold_timestamp")
# MAGIC     )
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC @dlt.table(
# MAGIC     name="dim_contract",
# MAGIC     comment="Contract dimension with cost growth derivation",
# MAGIC     table_properties={"quality": "gold", "layer": "gold"}
# MAGIC )
# MAGIC def gold_dim_contract():
# MAGIC     df = spark.read.table(f"{CATALOG}.procurement_silver.silver_contracts")
# MAGIC     return df.select(
# MAGIC         col("contract_id"),
# MAGIC         col("vendor_id"),
# MAGIC         col("project_id"),
# MAGIC         col("contract_type"),
# MAGIC         col("contract_status"),
# MAGIC         col("contract_status").alias("status"),
# MAGIC         col("contract_value"),
# MAGIC         col("contract_value").alias("original_value"),
# MAGIC         col("revised_value"),
# MAGIC         col("start_date"),
# MAGIC         col("end_date"),
# MAGIC         col("contract_duration_days"),
# MAGIC         col("payment_terms"),
# MAGIC         col("retention_pct"),
# MAGIC         col("performance_bond_pct").alias("performance_bond"),
# MAGIC         col("variation_orders").alias("change_order_count"),
# MAGIC         (col("revised_value") - col("contract_value")).alias("change_order_value"),
# MAGIC         when(col("contract_value") > 0,
# MAGIC             spark_round((col("revised_value") - col("contract_value")) / col("contract_value") * 100, 2)
# MAGIC         ).otherwise(lit(0.0)).alias("cost_growth_pct"),
# MAGIC         col("contract_manager_id").alias("approved_by"),
# MAGIC         lit(True).cast(BooleanType()).alias("_is_current"),
# MAGIC         current_timestamp().alias("_gold_timestamp")
# MAGIC     )
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC @dlt.table(
# MAGIC     name="dim_cost_center",
# MAGIC     comment="Cost center dimension - derived from employee data",
# MAGIC     table_properties={"quality": "gold", "layer": "gold"}
# MAGIC )
# MAGIC def gold_dim_cost_center():
# MAGIC     df = spark.read.table(f"{CATALOG}.procurement_silver.silver_employees")
# MAGIC     return df.select("cost_center", "department").distinct().select(
# MAGIC         col("cost_center").alias("cost_center_key"),
# MAGIC         col("cost_center").alias("cost_center_code"),
# MAGIC         col("department").alias("cost_center_category"),
# MAGIC         lit(True).cast(BooleanType()).alias("_is_current"),
# MAGIC         current_timestamp().alias("_gold_timestamp")
# MAGIC     )
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC @dlt.table(
# MAGIC     name="dim_sector",
# MAGIC     comment="Sector dimension - union of vendor and project sectors",
# MAGIC     table_properties={"quality": "gold", "layer": "gold"}
# MAGIC )
# MAGIC def gold_dim_sector():
# MAGIC     v_sec = spark.read.table(f"{CATALOG}.procurement_silver.silver_vendors").select(
# MAGIC         upper(trim(col("sector_specialization"))).alias("sector_name")
# MAGIC     )
# MAGIC     p_sec = spark.read.table(f"{CATALOG}.procurement_silver.silver_projects").select(
# MAGIC         upper(trim(col("sector"))).alias("sector_name")
# MAGIC     )
# MAGIC     return v_sec.union(p_sec).distinct().select(
# MAGIC         col("sector_name").alias("sector_key"),
# MAGIC         col("sector_name"),
# MAGIC         lit(True).cast(BooleanType()).alias("_is_current"),
# MAGIC         current_timestamp().alias("_gold_timestamp")
# MAGIC     )
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC %md
# MAGIC ## Facts
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC @dlt.table(
# MAGIC     name="fact_purchase_orders",
# MAGIC     comment="Purchase order facts with line-item aggregates",
# MAGIC     table_properties={"quality": "gold", "layer": "gold"}
# MAGIC )
# MAGIC def gold_fact_purchase_orders():
# MAGIC     po = spark.read.table(f"{CATALOG}.procurement_silver.silver_purchase_orders")
# MAGIC     poli_agg = (
# MAGIC         spark.read.table(f"{CATALOG}.procurement_silver.silver_po_line_items")
# MAGIC         .groupBy("po_id")
# MAGIC         .agg(
# MAGIC             spark_sum("line_value").alias("line_total"),
# MAGIC             spark_sum("qty_ordered").alias("total_quantity")
# MAGIC         )
# MAGIC     )
# MAGIC     df = po.join(poli_agg, "po_id", "left")
# MAGIC     return df.select(
# MAGIC         col("po_id"),
# MAGIC         col("vendor_id"),
# MAGIC         col("project_id"),
# MAGIC         col("po_date"),
# MAGIC         col("delivery_date"),
# MAGIC         col("delivery_date").alias("expected_delivery_date"),
# MAGIC         col("approval_date"),
# MAGIC         col("po_value"),
# MAGIC         col("tax_amount"),
# MAGIC         col("total_value"),
# MAGIC         col("total_value").alias("total_amount"),
# MAGIC         col("po_status"),
# MAGIC         col("po_type"),
# MAGIC         col("priority"),
# MAGIC         col("currency"),
# MAGIC         col("payment_terms"),
# MAGIC         coalesce(col("line_total"), lit(0)).alias("line_total"),
# MAGIC         coalesce(col("total_quantity"), lit(0)).alias("total_quantity"),
# MAGIC         current_timestamp().alias("_gold_timestamp")
# MAGIC     )
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC @dlt.table(
# MAGIC     name="fact_invoices",
# MAGIC     comment="Invoice facts with aging analysis",
# MAGIC     table_properties={"quality": "gold", "layer": "gold"}
# MAGIC )
# MAGIC def gold_fact_invoices():
# MAGIC     df = spark.read.table(f"{CATALOG}.procurement_silver.silver_invoices")
# MAGIC     return df.select(
# MAGIC         col("invoice_id"),
# MAGIC         col("vendor_id"),
# MAGIC         col("po_id"),
# MAGIC         col("project_id"),
# MAGIC         col("contract_id"),
# MAGIC         col("invoice_ref").alias("invoice_number"),
# MAGIC         col("invoice_type"),
# MAGIC         when(col("payment_date").isNotNull(), lit("PAID"))
# MAGIC             .when(col("due_date") < current_date(), lit("OVERDUE"))
# MAGIC             .otherwise(lit("PENDING")).alias("payment_status"),
# MAGIC         col("invoice_date"),
# MAGIC         col("due_date"),
# MAGIC         col("payment_date"),
# MAGIC         col("invoice_amount"),
# MAGIC         col("invoice_amount").alias("net_amount"),
# MAGIC         col("tax_rate"),
# MAGIC         col("tax_amount"),
# MAGIC         col("total_amount"),
# MAGIC         col("total_amount").alias("gross_amount"),
# MAGIC         col("paid_amount"),
# MAGIC         col("retention_amount"),
# MAGIC         col("days_to_pay"),
# MAGIC         col("days_overdue"),
# MAGIC         col("days_overdue").alias("days_outstanding"),
# MAGIC         when(col("days_overdue") <= 0, lit("CURRENT"))
# MAGIC             .when(col("days_overdue") <= 30, lit("1-30 DAYS"))
# MAGIC             .when(col("days_overdue") <= 60, lit("31-60 DAYS"))
# MAGIC             .when(col("days_overdue") <= 90, lit("61-90 DAYS"))
# MAGIC             .otherwise(lit("90+ DAYS")).alias("aging_bucket"),
# MAGIC         col("currency"),
# MAGIC         col("approved_by"),
# MAGIC         col("description").alias("notes"),
# MAGIC         current_timestamp().alias("_gold_timestamp")
# MAGIC     )
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC @dlt.table(
# MAGIC     name="fact_goods_receipts",
# MAGIC     comment="Goods receipt facts with quality metrics",
# MAGIC     table_properties={"quality": "gold", "layer": "gold"}
# MAGIC )
# MAGIC def gold_fact_goods_receipts():
# MAGIC     gr = spark.read.table(f"{CATALOG}.procurement_silver.silver_goods_receipts")
# MAGIC     po = spark.read.table(f"{CATALOG}.procurement_silver.silver_purchase_orders").select(
# MAGIC         col("po_id").alias("_po_id")
# MAGIC     )
# MAGIC     poli = spark.read.table(f"{CATALOG}.procurement_silver.silver_po_line_items").select(
# MAGIC         col("line_id").alias("_poli_line_id"),
# MAGIC         col("unit_price").alias("_poli_unit_price"),
# MAGIC     )
# MAGIC     df = (
# MAGIC         gr.join(po, gr["po_id"] == po["_po_id"], "left")
# MAGIC           .drop("_po_id")
# MAGIC           .join(poli, gr["line_item_id"] == poli["_poli_line_id"], "left")
# MAGIC           .drop("_poli_line_id")
# MAGIC     )
# MAGIC     return df.select(
# MAGIC         gr["grn_id"],
# MAGIC         gr["po_id"],
# MAGIC         gr["line_item_id"],
# MAGIC         gr["vendor_id"],
# MAGIC         gr["material_id"],
# MAGIC         gr["project_id"],
# MAGIC         gr["receipt_date"],
# MAGIC         col("qty_received").alias("quantity_received"),
# MAGIC         col("qty_accepted").alias("quantity_accepted"),
# MAGIC         col("qty_rejected").alias("quantity_rejected"),
# MAGIC         col("rejection_rate"),
# MAGIC         (lit(100) - col("rejection_rate")).alias("acceptance_rate"),
# MAGIC         (col("qty_accepted") * coalesce(col("_poli_unit_price"), lit(0))).alias("receipt_value"),
# MAGIC         col("grn_status"),
# MAGIC         col("storage_location"),
# MAGIC         col("received_by").alias("inspector"),
# MAGIC         current_timestamp().alias("_gold_timestamp")
# MAGIC     )
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC @dlt.table(
# MAGIC     name="fact_project_costs",
# MAGIC     comment="Project cost facts - budget vs actual with variance",
# MAGIC     table_properties={"quality": "gold", "layer": "gold"}
# MAGIC )
# MAGIC def gold_fact_project_costs():
# MAGIC     budgets = spark.read.table(f"{CATALOG}.procurement_silver.silver_project_budgets")
# MAGIC     actuals_agg = (
# MAGIC         spark.read.table(f"{CATALOG}.procurement_silver.silver_project_actuals")
# MAGIC         .groupBy("project_id", "wbs_code", "cost_type")
# MAGIC         .agg(
# MAGIC             spark_sum("actual_amount").alias("_act_total"),
# MAGIC             spark_max("posting_date").alias("_act_posting_date")
# MAGIC         )
# MAGIC     )
# MAGIC     df = (
# MAGIC         budgets.alias("b")
# MAGIC         .join(
# MAGIC             actuals_agg.alias("a"),
# MAGIC             (col("b.project_id") == col("a.project_id"))
# MAGIC             & (col("b.wbs_code") == col("a.wbs_code"))
# MAGIC             & (upper(col("b.cost_type")) == col("a.cost_type")),
# MAGIC             "left",
# MAGIC         )
# MAGIC     )
# MAGIC     budget_col = col("b.revised_amount")
# MAGIC     actual_col = coalesce(col("a._act_total"), col("b.actual_spent"))
# MAGIC     return df.select(
# MAGIC         col("b.budget_id"),
# MAGIC         col("b.project_id"),
# MAGIC         col("b.wbs_code").alias("wbs_element"),
# MAGIC         col("b.wbs_code"),
# MAGIC         col("b.cost_type"),
# MAGIC         budget_col.alias("budget_amount"),
# MAGIC         col("b.original_amount").alias("original_budget"),
# MAGIC         col("b.committed_amount"),
# MAGIC         actual_col.alias("actual_amount"),
# MAGIC         (budget_col - actual_col).alias("remaining_amount"),
# MAGIC         when(budget_col > 0,
# MAGIC             spark_round(actual_col / budget_col * 100, 2)
# MAGIC         ).otherwise(lit(0.0)).alias("utilization_pct"),
# MAGIC         (budget_col - actual_col).alias("variance_amount"),
# MAGIC         when(budget_col > 0,
# MAGIC             spark_round((budget_col - actual_col) / budget_col * 100, 2)
# MAGIC         ).otherwise(lit(0.0)).alias("variance_pct"),
# MAGIC         col("b.budget_status"),
# MAGIC         col("b.period_start"),
# MAGIC         col("b.period_end"),
# MAGIC         col("a._act_posting_date").alias("posting_date"),
# MAGIC         current_timestamp().alias("_gold_timestamp")
# MAGIC     )
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC @dlt.table(
# MAGIC     name="fact_project_actuals",
# MAGIC     comment="Project actuals detail facts",
# MAGIC     table_properties={"quality": "gold", "layer": "gold"}
# MAGIC )
# MAGIC def gold_fact_project_actuals():
# MAGIC     df = spark.read.table(f"{CATALOG}.procurement_silver.silver_project_actuals")
# MAGIC     return df.select(
# MAGIC         col("transaction_id"),
# MAGIC         col("po_id"),
# MAGIC         col("invoice_id"),
# MAGIC         col("wbs_code"),
# MAGIC         col("cost_type"),
# MAGIC         col("cost_category"),
# MAGIC         col("cost_status"),
# MAGIC         col("transaction_date"),
# MAGIC         col("posting_date"),
# MAGIC         col("actual_amount"),
# MAGIC         col("budget_amount"),
# MAGIC         col("variance"),
# MAGIC         col("fiscal_period"),
# MAGIC         current_timestamp().alias("_gold_timestamp")
# MAGIC     )
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC @dlt.table(
# MAGIC     name="fact_vendor_performance",
# MAGIC     comment="Vendor performance evaluation facts",
# MAGIC     table_properties={"quality": "gold", "layer": "gold"}
# MAGIC )
# MAGIC def gold_fact_vendor_performance():
# MAGIC     df = spark.read.table(f"{CATALOG}.procurement_silver.silver_vendor_performance")
# MAGIC     return df.select(
# MAGIC         col("performance_id"),
# MAGIC         col("vendor_id"),
# MAGIC         col("project_id"),
# MAGIC         col("evaluation_date"),
# MAGIC         col("evaluation_period"),
# MAGIC         col("delivery_score"),
# MAGIC         col("quality_score"),
# MAGIC         col("commercial_score"),
# MAGIC         col("hse_score"),
# MAGIC         col("compliance_score"),
# MAGIC         col("overall_score"),
# MAGIC         col("recommendation"),
# MAGIC         col("evaluator_id"),
# MAGIC         col("remarks").alias("notes"),
# MAGIC         current_timestamp().alias("_gold_timestamp")
# MAGIC     )
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC @dlt.table(
# MAGIC     name="fact_inventory",
# MAGIC     comment="Inventory movement facts",
# MAGIC     table_properties={"quality": "gold", "layer": "gold"}
# MAGIC )
# MAGIC def gold_fact_inventory():
# MAGIC     df = spark.read.table(f"{CATALOG}.procurement_silver.silver_inventory")
# MAGIC     return df.select(
# MAGIC         col("movement_id"),
# MAGIC         col("material_id"),
# MAGIC         col("grn_id"),
# MAGIC         col("movement_type"),
# MAGIC         col("movement_date"),
# MAGIC         col("quantity"),
# MAGIC         col("unit_cost"),
# MAGIC         col("total_value"),
# MAGIC         col("from_location"),
# MAGIC         col("to_location"),
# MAGIC         col("batch_number"),
# MAGIC         col("unit_of_measure"),
# MAGIC         col("reason_code"),
# MAGIC         col("posted_by"),
# MAGIC         current_timestamp().alias("_gold_timestamp")
# MAGIC     )
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC @dlt.table(
# MAGIC     name="fact_contracts",
# MAGIC     comment="Contract facts with item aggregates",
# MAGIC     table_properties={"quality": "gold", "layer": "gold"}
# MAGIC )
# MAGIC def gold_fact_contracts():
# MAGIC     contracts = spark.read.table(f"{CATALOG}.procurement_silver.silver_contracts")
# MAGIC     items_agg = (
# MAGIC         spark.read.table(f"{CATALOG}.procurement_silver.silver_contract_items")
# MAGIC         .groupBy("contract_id")
# MAGIC         .agg(
# MAGIC             spark_sum("line_value").alias("items_total"),
# MAGIC             spark_count("item_id").alias("item_count")
# MAGIC         )
# MAGIC     )
# MAGIC     df = contracts.join(items_agg, "contract_id", "left")
# MAGIC     return df.select(
# MAGIC         col("contract_id"),
# MAGIC         col("vendor_id"),
# MAGIC         col("project_id"),
# MAGIC         col("contract_type"),
# MAGIC         col("contract_status"),
# MAGIC         col("contract_value"),
# MAGIC         col("revised_value"),
# MAGIC         col("start_date"),
# MAGIC         col("variation_orders").alias("change_order_count"),
# MAGIC         (col("revised_value") - col("contract_value")).alias("change_order_value"),
# MAGIC         coalesce(col("items_total"), lit(0)).alias("items_total"),
# MAGIC         coalesce(col("item_count"), lit(0)).alias("item_count"),
# MAGIC         when(col("contract_value") > 0,
# MAGIC             spark_round(coalesce(col("items_total"), lit(0)) / col("contract_value") * 100, 2)
# MAGIC         ).otherwise(lit(0.0)).alias("value_utilisation_pct"),
# MAGIC         col("payment_terms"),
# MAGIC         col("retention_pct"),
# MAGIC         col("liquidated_damages_pct").alias("penalty_clause"),
# MAGIC         col("performance_bond_pct").alias("performance_bond"),
# MAGIC         col("currency"),
# MAGIC         current_timestamp().alias("_gold_timestamp")
# MAGIC     )
# MAGIC # COMMAND ----------
# MAGIC @dlt.table(
# MAGIC     name="fact_sales",
# MAGIC     comment="Sales order facts",
# MAGIC     table_properties={"quality": "gold", "layer": "gold"}
# MAGIC )
# MAGIC def gold_fact_sales():
# MAGIC     sales = spark.read.table(f"{CATALOG}.procurement_silver.silver_sales_orders")
# MAGIC     materials = spark.read.table(f"{CATALOG}.procurement_silver.silver_materials").select(
# MAGIC         col("material_id").alias("_mat_id"),
# MAGIC         col("material_name").alias("product_name")
# MAGIC     )
# MAGIC     df = sales.join(materials, sales["material_id"] == materials["_mat_id"], "left").drop("_mat_id")
# MAGIC     return df.select(
# MAGIC         col("sales_order_id"),
# MAGIC         col("product_name"),
# MAGIC         col("customer_name"),
# MAGIC         sha2(col("customer_name"), 256).alias("customer_id"),
# MAGIC         col("customer_type"),
# MAGIC         col("order_type"),
# MAGIC         col("order_status"),
# MAGIC         col("payment_status"),
# MAGIC         col("order_date"),
# MAGIC         col("delivery_date"),
# MAGIC         col("quantity"),
# MAGIC         col("unit_price"),
# MAGIC         col("revenue"),
# MAGIC         col("cogs"),
# MAGIC         col("gross_margin"),
# MAGIC         col("margin_pct"),
# MAGIC         col("tax_amount"),
# MAGIC         col("total_amount"),
# MAGIC         col("currency"),
# MAGIC         col("delivery_terms"),
# MAGIC         col("_silver_timestamp").alias("load_timestamp")
# MAGIC     )