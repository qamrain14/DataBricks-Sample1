# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Cleansed & Enriched Data
# MAGIC Construction & Oil/Gas Procurement Lakehouse
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC import dlt
# MAGIC from pyspark.sql.functions import (
# MAGIC     col, when, trim, upper, lower, coalesce, lit, current_timestamp,
# MAGIC     to_date, datediff, round as spark_round, concat, sha2, regexp_replace
# MAGIC )
# MAGIC from pyspark.sql.types import DoubleType, IntegerType, DateType
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC CATALOG = spark.conf.get("catalog", "procurement_lakehouse")
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC %md
# MAGIC ## 1. Silver Vendors
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC @dlt.table(
# MAGIC     name="silver_vendors",
# MAGIC     comment="Cleansed vendor master data with standardized fields",
# MAGIC     table_properties={"quality": "silver"}
# MAGIC )
# MAGIC @dlt.expect_or_drop("valid_vendor_id", "vendor_id IS NOT NULL")
# MAGIC @dlt.expect_or_drop("valid_vendor_name", "vendor_name IS NOT NULL")
# MAGIC @dlt.expect("valid_rating", "performance_rating BETWEEN 0 AND 5")
# MAGIC def silver_vendors():
# MAGIC     return (
# MAGIC         spark.read.table(f"{CATALOG}.procurement_bronze.bronze_vendors")
# MAGIC         .withColumn("vendor_name", trim(col("vendor_name")))
# MAGIC         .withColumn("country", upper(trim(col("country"))))
# MAGIC         .withColumn("state", upper(trim(col("state"))))
# MAGIC         .withColumn("city", trim(col("city")))
# MAGIC         .withColumn("performance_rating", col("performance_rating").cast(DoubleType()))
# MAGIC         .withColumn("credit_limit_usd", col("credit_limit_usd").cast(DoubleType()))
# MAGIC         .withColumn("annual_turnover_usd", col("annual_turnover_usd").cast(DoubleType()))
# MAGIC         .withColumn("payment_terms_days", col("payment_terms_days").cast(IntegerType()))
# MAGIC         .withColumn("_silver_timestamp", current_timestamp())
# MAGIC         .drop("_ingest_timestamp", "_source_file")
# MAGIC     )
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC %md
# MAGIC ## 2. Silver Projects
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC @dlt.table(
# MAGIC     name="silver_projects",
# MAGIC     comment="Cleansed project data with derived fields",
# MAGIC     table_properties={"quality": "silver"}
# MAGIC )
# MAGIC @dlt.expect_or_drop("valid_project_id", "project_id IS NOT NULL")
# MAGIC @dlt.expect("valid_budget", "approved_budget_usd > 0")
# MAGIC def silver_projects():
# MAGIC     return (
# MAGIC         spark.read.table(f"{CATALOG}.procurement_bronze.bronze_projects")
# MAGIC         .withColumn("project_name", trim(col("project_name")))
# MAGIC         .withColumn("country", upper(trim(col("country"))))
# MAGIC         .withColumn("contract_value_usd", col("contract_value_usd").cast(DoubleType()))
# MAGIC         .withColumn("approved_budget_usd", col("approved_budget_usd").cast(DoubleType()))
# MAGIC         .withColumn("start_date", to_date(col("start_date")))
# MAGIC         .withColumn("planned_completion_date", to_date(col("planned_completion_date")))
# MAGIC         .withColumn("actual_completion_date", to_date(col("actual_completion_date")))
# MAGIC         .withColumn("planned_duration_days",
# MAGIC             datediff(col("planned_completion_date"), col("start_date")))
# MAGIC         .withColumn("budget_variance_pct",
# MAGIC             when(col("approved_budget_usd") > 0,
# MAGIC                 spark_round((col("contract_value_usd") - col("approved_budget_usd"))
# MAGIC                     / col("approved_budget_usd") * 100, 2))
# MAGIC             .otherwise(lit(0.0)))
# MAGIC         .withColumn("_silver_timestamp", current_timestamp())
# MAGIC         .drop("_ingest_timestamp", "_source_file")
# MAGIC     )
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC %md
# MAGIC ## 3. Silver Materials
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC @dlt.table(
# MAGIC     name="silver_materials",
# MAGIC     comment="Cleansed material catalog with standardized categories",
# MAGIC     table_properties={"quality": "silver"}
# MAGIC )
# MAGIC @dlt.expect_or_drop("valid_material_id", "material_id IS NOT NULL")
# MAGIC @dlt.expect("valid_price", "unit_price >= 0")
# MAGIC def silver_materials():
# MAGIC     return (
# MAGIC         spark.read.table(f"{CATALOG}.procurement_bronze.bronze_materials")
# MAGIC         .withColumn("material_name", trim(col("material_name")))
# MAGIC         .withColumn("category", upper(trim(col("category"))))
# MAGIC         .withColumn("sub_category", trim(col("sub_category")))
# MAGIC         .withColumn("unit_price", col("unit_price").cast(DoubleType()))
# MAGIC         .withColumn("weight_kg", col("weight_kg").cast(DoubleType()))
# MAGIC         .withColumn("lead_time_days", col("lead_time_days").cast(IntegerType()))
# MAGIC         .withColumn("is_active", col("active").cast("boolean"))
# MAGIC         .withColumn("_silver_timestamp", current_timestamp())
# MAGIC         .drop("_ingest_timestamp", "_source_file", "active")
# MAGIC     )
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC %md
# MAGIC ## 4. Silver Employees
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC @dlt.table(
# MAGIC     name="silver_employees",
# MAGIC     comment="Cleansed employee data",
# MAGIC     table_properties={"quality": "silver"}
# MAGIC )
# MAGIC @dlt.expect_or_drop("valid_employee_id", "employee_id IS NOT NULL")
# MAGIC def silver_employees():
# MAGIC     return (
# MAGIC         spark.read.table(f"{CATALOG}.procurement_bronze.bronze_employees")
# MAGIC         .withColumn("full_name", concat(trim(col("first_name")), lit(" "), trim(col("last_name"))))
# MAGIC         .withColumn("department", upper(trim(col("department"))))
# MAGIC         .withColumn("is_active", col("active").cast("boolean"))
# MAGIC         .withColumn("approval_limit", col("approval_limit").cast(DoubleType()))
# MAGIC         .withColumn("hire_date", to_date(col("hire_date")))
# MAGIC         .withColumn("_silver_timestamp", current_timestamp())
# MAGIC         .drop("_ingest_timestamp", "_source_file", "active")
# MAGIC     )
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC %md
# MAGIC ## 5. Silver Purchase Orders
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC @dlt.table(
# MAGIC     name="silver_purchase_orders",
# MAGIC     comment="Cleansed purchase order data with enrichments",
# MAGIC     table_properties={"quality": "silver"}
# MAGIC )
# MAGIC @dlt.expect_or_drop("valid_po_id", "po_id IS NOT NULL")
# MAGIC @dlt.expect_or_drop("valid_vendor_ref", "vendor_id IS NOT NULL")
# MAGIC @dlt.expect("valid_po_value", "po_value >= 0")
# MAGIC def silver_purchase_orders():
# MAGIC     return (
# MAGIC         spark.read.table(f"{CATALOG}.procurement_bronze.bronze_purchase_orders")
# MAGIC         .withColumn("po_date", to_date(col("po_date")))
# MAGIC         .withColumn("delivery_date", to_date(col("delivery_date")))
# MAGIC         .withColumn("approval_date", to_date(col("approval_date")))
# MAGIC         .withColumn("po_value", col("po_value").cast(DoubleType()))
# MAGIC         .withColumn("tax_amount", col("tax_amount").cast(DoubleType()))
# MAGIC         .withColumn("total_value", col("total_value").cast(DoubleType()))
# MAGIC         .withColumn("po_status", upper(trim(col("po_status"))))
# MAGIC         .withColumn("order_date", to_date(col("po_date")))
# MAGIC         .withColumn("_silver_timestamp", current_timestamp())
# MAGIC         .drop("_ingest_timestamp", "_source_file")
# MAGIC     )
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC %md
# MAGIC ## 6. Silver PO Line Items
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC @dlt.table(
# MAGIC     name="silver_po_line_items",
# MAGIC     comment="Cleansed PO line items with calculated fields",
# MAGIC     table_properties={"quality": "silver"}
# MAGIC )
# MAGIC @dlt.expect_or_drop("valid_line_id", "line_item_id IS NOT NULL")
# MAGIC @dlt.expect_or_drop("valid_po_ref", "po_id IS NOT NULL")
# MAGIC def silver_po_line_items():
# MAGIC     return (
# MAGIC         spark.read.table(f"{CATALOG}.procurement_bronze.bronze_po_line_items")
# MAGIC         .withColumn("qty_ordered", col("qty_ordered").cast(DoubleType()))
# MAGIC         .withColumn("qty_received", col("qty_received").cast(DoubleType()))
# MAGIC         .withColumn("qty_accepted", col("qty_accepted").cast(DoubleType()))
# MAGIC         .withColumn("unit_price", col("unit_price").cast(DoubleType()))
# MAGIC         .withColumn("line_value", col("line_value").cast(DoubleType()))
# MAGIC         .withColumn("delivery_date", to_date(col("delivery_date")))
# MAGIC         .withColumn("actual_delivery_date", to_date(col("actual_delivery_date")))
# MAGIC         .withColumn("fulfillment_rate",
# MAGIC             when(col("qty_ordered") > 0,
# MAGIC                 spark_round(col("qty_received") / col("qty_ordered") * 100, 2))
# MAGIC             .otherwise(lit(0.0)))
# MAGIC         .withColumn("acceptance_rate",
# MAGIC             when(col("qty_received") > 0,
# MAGIC                 spark_round(col("qty_accepted") / col("qty_received") * 100, 2))
# MAGIC             .otherwise(lit(0.0)))
# MAGIC         .withColumn("_silver_timestamp", current_timestamp())
# MAGIC         .drop("_ingest_timestamp", "_source_file")
# MAGIC     )
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC %md
# MAGIC ## 7. Silver Contracts
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC @dlt.table(
# MAGIC     name="silver_contracts",
# MAGIC     comment="Cleansed contract data",
# MAGIC     table_properties={"quality": "silver"}
# MAGIC )
# MAGIC @dlt.expect_or_drop("valid_contract_id", "contract_id IS NOT NULL")
# MAGIC @dlt.expect("valid_value", "original_value > 0")
# MAGIC def silver_contracts():
# MAGIC     return (
# MAGIC         spark.read.table(f"{CATALOG}.procurement_bronze.bronze_contracts")
# MAGIC         .withColumn("contract_value", col("original_value").cast(DoubleType()))
# MAGIC         .withColumn("start_date", to_date(col("start_date")))
# MAGIC         .withColumn("end_date", to_date(col("end_date")))
# MAGIC         .withColumn("contract_status", upper(trim(col("contract_status"))))
# MAGIC         .withColumn("contract_type", upper(trim(col("contract_type"))))
# MAGIC         .withColumn("contract_duration_days",
# MAGIC             datediff(col("end_date"), col("start_date")))
# MAGIC         .withColumn("_silver_timestamp", current_timestamp())
# MAGIC         .drop("_ingest_timestamp", "_source_file")
# MAGIC     )
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC %md
# MAGIC ## 8. Silver Contract Items
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC @dlt.table(
# MAGIC     name="silver_contract_items",
# MAGIC     comment="Cleansed contract line items",
# MAGIC     table_properties={"quality": "silver"}
# MAGIC )
# MAGIC @dlt.expect_or_drop("valid_item_id", "item_id IS NOT NULL")
# MAGIC @dlt.expect_or_drop("valid_contract_ref", "contract_id IS NOT NULL")
# MAGIC def silver_contract_items():
# MAGIC     return (
# MAGIC         spark.read.table(f"{CATALOG}.procurement_bronze.bronze_contract_items")
# MAGIC         .withColumn("quantity", col("quantity").cast(DoubleType()))
# MAGIC         .withColumn("unit_price", col("unit_rate").cast(DoubleType()))
# MAGIC         .withColumn("line_value", col("amount").cast(DoubleType()))
# MAGIC         .withColumn("_silver_timestamp", current_timestamp())
# MAGIC         .drop("_ingest_timestamp", "_source_file")
# MAGIC     )
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC %md
# MAGIC ## 9. Silver Invoices
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC @dlt.table(
# MAGIC     name="silver_invoices",
# MAGIC     comment="Cleansed invoice data with payment analysis",
# MAGIC     table_properties={"quality": "silver"}
# MAGIC )
# MAGIC @dlt.expect_or_drop("valid_invoice_id", "invoice_id IS NOT NULL")
# MAGIC @dlt.expect("valid_amount", "gross_amount > 0")
# MAGIC def silver_invoices():
# MAGIC     return (
# MAGIC         spark.read.table(f"{CATALOG}.procurement_bronze.bronze_invoices")
# MAGIC         .withColumn("invoice_date", to_date(col("invoice_date")))
# MAGIC         .withColumn("due_date", to_date(col("due_date")))
# MAGIC         .withColumn("payment_date", to_date(col("paid_date")))
# MAGIC         .withColumn("invoice_amount", col("gross_amount").cast(DoubleType()))
# MAGIC         .withColumn("tax_amount", col("tax_amount").cast(DoubleType()))
# MAGIC         .withColumn("total_amount", col("net_amount").cast(DoubleType()))
# MAGIC         .withColumn("paid_amount", col("paid_amount").cast(DoubleType()))
# MAGIC         .withColumn("invoice_status", upper(trim(col("invoice_status"))))
# MAGIC         .withColumn("days_to_pay",
# MAGIC             when(col("payment_date").isNotNull(),
# MAGIC                 datediff(col("payment_date"), col("invoice_date")))
# MAGIC             .otherwise(lit(None)))
# MAGIC         .withColumn("days_overdue",
# MAGIC             when((col("payment_date").isNull()) & (col("due_date").isNotNull()),
# MAGIC                 datediff(current_timestamp(), col("due_date")))
# MAGIC             .otherwise(lit(0)))
# MAGIC         .withColumn("_silver_timestamp", current_timestamp())
# MAGIC         .drop("_ingest_timestamp", "_source_file")
# MAGIC     )
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC %md
# MAGIC ## 10. Silver Goods Receipts
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC @dlt.table(
# MAGIC     name="silver_goods_receipts",
# MAGIC     comment="Cleansed goods receipt data",
# MAGIC     table_properties={"quality": "silver"}
# MAGIC )
# MAGIC @dlt.expect_or_drop("valid_grn_id", "grn_id IS NOT NULL")
# MAGIC @dlt.expect_or_drop("valid_po_ref", "po_id IS NOT NULL")
# MAGIC def silver_goods_receipts():
# MAGIC     return (
# MAGIC         spark.read.table(f"{CATALOG}.procurement_bronze.bronze_goods_receipts")
# MAGIC         .withColumn("receipt_date", to_date(col("receipt_date")))
# MAGIC         .withColumn("qty_received", col("qty_delivered").cast(DoubleType()))
# MAGIC         .withColumn("qty_accepted", col("qty_accepted").cast(DoubleType()))
# MAGIC         .withColumn("qty_rejected", col("qty_rejected").cast(DoubleType()))
# MAGIC         .withColumn("rejection_rate",
# MAGIC             when(col("qty_delivered") > 0,
# MAGIC                 spark_round(col("qty_rejected") / col("qty_delivered") * 100, 2))
# MAGIC             .otherwise(lit(0.0)))
# MAGIC         .withColumn("grn_status", upper(trim(col("grn_status"))))
# MAGIC         .withColumn("storage_location", trim(col("storage_location")))
# MAGIC         .withColumn("_silver_timestamp", current_timestamp())
# MAGIC         .drop("_ingest_timestamp", "_source_file")
# MAGIC     )
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC %md
# MAGIC ## 11. Silver Project Budgets
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC @dlt.table(
# MAGIC     name="silver_project_budgets",
# MAGIC     comment="Cleansed project budget allocations",
# MAGIC     table_properties={"quality": "silver"}
# MAGIC )
# MAGIC @dlt.expect_or_drop("valid_budget_id", "budget_id IS NOT NULL")
# MAGIC @dlt.expect("valid_budget_amount", "revised_amount >= 0")
# MAGIC def silver_project_budgets():
# MAGIC     return (
# MAGIC         spark.read.table(f"{CATALOG}.procurement_bronze.bronze_project_budgets")
# MAGIC         .withColumn("budget_amount", col("revised_amount").cast(DoubleType()))
# MAGIC         .withColumn("original_budget", col("original_amount").cast(DoubleType()))
# MAGIC         .withColumn("committed_amount", col("committed_amount").cast(DoubleType()))
# MAGIC         .withColumn("spent_amount", col("actual_spent").cast(DoubleType()))
# MAGIC         .withColumn("remaining_amount",
# MAGIC             col("revised_amount").cast(DoubleType()) - col("actual_spent").cast(DoubleType()))
# MAGIC         .withColumn("utilization_pct",
# MAGIC             when(col("revised_amount") > 0,
# MAGIC                 spark_round(col("actual_spent").cast(DoubleType()) / col("revised_amount").cast(DoubleType()) * 100, 2))
# MAGIC             .otherwise(lit(0.0)))
# MAGIC         .withColumn("wbs_code", trim(col("wbs_code")))
# MAGIC         .withColumn("cost_type", upper(trim(col("cost_type"))))
# MAGIC         .withColumn("budget_status", upper(trim(col("budget_status"))))
# MAGIC         .withColumn("period_start", to_date(col("period_start")))
# MAGIC         .withColumn("period_end", to_date(col("period_end")))
# MAGIC         .withColumn("_silver_timestamp", current_timestamp())
# MAGIC         .drop("_ingest_timestamp", "_source_file")
# MAGIC     )
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC %md
# MAGIC ## 12. Silver Project Actuals
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC @dlt.table(
# MAGIC     name="silver_project_actuals",
# MAGIC     comment="Cleansed project actual transactions",
# MAGIC     table_properties={"quality": "silver"}
# MAGIC )
# MAGIC @dlt.expect_or_drop("valid_transaction_id", "actual_id IS NOT NULL")
# MAGIC @dlt.expect("valid_amount", "actual_amount > 0")
# MAGIC def silver_project_actuals():
# MAGIC     return (
# MAGIC         spark.read.table(f"{CATALOG}.procurement_bronze.bronze_project_actuals")
# MAGIC         .withColumn("transaction_date", to_date(col("transaction_date")))
# MAGIC         .withColumn("posting_date", to_date(col("posting_date")))
# MAGIC         .withColumn("amount", col("actual_amount").cast(DoubleType()))
# MAGIC         .withColumn("actual_amount", col("actual_amount").cast(DoubleType()))
# MAGIC         .withColumn("budget_amount", col("budget_amount").cast(DoubleType()))
# MAGIC         .withColumn("variance", col("variance").cast(DoubleType()))
# MAGIC         .withColumn("cost_category", upper(trim(col("cost_type"))))
# MAGIC         .withColumn("cost_type", upper(trim(col("cost_type"))))
# MAGIC         .withColumn("cost_status", upper(trim(col("cost_status"))))
# MAGIC         .withColumn("wbs_code", trim(col("wbs_code")))
# MAGIC         .withColumn("fiscal_period", trim(col("fiscal_period")))
# MAGIC         .withColumn("_silver_timestamp", current_timestamp())
# MAGIC         .drop("_ingest_timestamp", "_source_file")
# MAGIC     )
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC %md
# MAGIC ## 13. Silver Sales Orders
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC @dlt.table(
# MAGIC     name="silver_sales_orders",
# MAGIC     comment="Cleansed sales order data",
# MAGIC     table_properties={"quality": "silver"}
# MAGIC )
# MAGIC @dlt.expect_or_drop("valid_sales_order_id", "order_id IS NOT NULL")
# MAGIC @dlt.expect("valid_order_value", "revenue > 0")
# MAGIC def silver_sales_orders():
# MAGIC     return (
# MAGIC         spark.read.table(f"{CATALOG}.procurement_bronze.bronze_sales_orders")
# MAGIC         .withColumn("order_date", to_date(col("order_date")))
# MAGIC         .withColumn("delivery_date", to_date(col("delivery_date")))
# MAGIC         .withColumn("order_value", col("revenue").cast(DoubleType()))
# MAGIC         .withColumn("revenue", col("revenue").cast(DoubleType()))
# MAGIC         .withColumn("tax_amount", col("tax_amount").cast(DoubleType()))
# MAGIC         .withColumn("total_value", col("total_amount").cast(DoubleType()))
# MAGIC         .withColumn("order_status", upper(trim(col("order_status"))))
# MAGIC         .withColumn("payment_status", upper(trim(col("payment_status"))))
# MAGIC         .withColumn("order_type", upper(trim(col("order_type"))))
# MAGIC         .withColumn("customer_name", trim(col("customer_name")))
# MAGIC         .withColumn("customer_type", upper(trim(col("customer_type"))))
# MAGIC         .withColumn("quantity", col("quantity").cast(DoubleType()))
# MAGIC         .withColumn("unit_price", col("unit_price").cast(DoubleType()))
# MAGIC         .withColumn("cogs", col("cogs").cast(DoubleType()))
# MAGIC         .withColumn("gross_margin", col("gross_margin").cast(DoubleType()))
# MAGIC         .withColumn("margin_pct", col("margin_pct").cast(DoubleType()))
# MAGIC         .withColumn("_silver_timestamp", current_timestamp())
# MAGIC         .drop("_ingest_timestamp", "_source_file")
# MAGIC     )
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC %md
# MAGIC ## 14. Silver Inventory
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC @dlt.table(
# MAGIC     name="silver_inventory",
# MAGIC     comment="Cleansed inventory movement data",
# MAGIC     table_properties={"quality": "silver"}
# MAGIC )
# MAGIC @dlt.expect_or_drop("valid_movement_id", "movement_id IS NOT NULL")
# MAGIC def silver_inventory():
# MAGIC     return (
# MAGIC         spark.read.table(f"{CATALOG}.procurement_bronze.bronze_inventory")
# MAGIC         .withColumn("movement_date", to_date(col("movement_date")))
# MAGIC         .withColumn("quantity", col("quantity").cast(DoubleType()))
# MAGIC         .withColumn("unit_cost", col("unit_cost").cast(DoubleType()))
# MAGIC         .withColumn("total_cost", col("total_value").cast(DoubleType()))
# MAGIC         .withColumn("movement_type", upper(trim(col("movement_type"))))
# MAGIC         .withColumn("from_location", trim(col("from_location")))
# MAGIC         .withColumn("to_location", trim(col("to_location")))
# MAGIC         .withColumn("unit_of_measure", upper(trim(col("unit_of_measure"))))
# MAGIC         .withColumn("reason_code", upper(trim(col("reason_code"))))
# MAGIC         .withColumn("_silver_timestamp", current_timestamp())
# MAGIC         .drop("_ingest_timestamp", "_source_file")
# MAGIC     )
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC %md
# MAGIC ## 15. Silver Vendor Performance
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC @dlt.table(
# MAGIC     name="silver_vendor_performance",
# MAGIC     comment="Cleansed vendor performance metrics",
# MAGIC     table_properties={"quality": "silver"}
# MAGIC )
# MAGIC @dlt.expect_or_drop("valid_performance_id", "performance_id IS NOT NULL")
# MAGIC @dlt.expect_or_drop("valid_vendor_ref", "vendor_id IS NOT NULL")
# MAGIC def silver_vendor_performance():
# MAGIC     return (
# MAGIC         spark.read.table(f"{CATALOG}.procurement_bronze.bronze_vendor_performance")
# MAGIC         .withColumn("evaluation_date", to_date(col("evaluation_date")))
# MAGIC         .withColumn("delivery_score", col("delivery_score").cast(DoubleType()))
# MAGIC         .withColumn("quality_score", col("quality_score").cast(DoubleType()))
# MAGIC         .withColumn("commercial_score", col("commercial_score").cast(DoubleType()))
# MAGIC         .withColumn("hse_score", col("hse_score").cast(DoubleType()))
# MAGIC         .withColumn("compliance_score", ((col("delivery_score") + col("quality_score") + col("commercial_score") + col("hse_score")) / lit(4)).cast(DoubleType()))
# MAGIC         .withColumn("overall_score", col("composite_score").cast(DoubleType()))
# MAGIC         .withColumn("recommendation", trim(col("recommendation")))
# MAGIC         .withColumn("evaluation_period", upper(trim(col("evaluation_period"))))
# MAGIC         .withColumn("_silver_timestamp", current_timestamp())
# MAGIC         .drop("_ingest_timestamp", "_source_file", "composite_score")
# MAGIC     )