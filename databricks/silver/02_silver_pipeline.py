# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Cleansed & Enriched Data
# MAGIC Construction & Oil/Gas Procurement Lakehouse

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, when, trim, upper, lower, coalesce, lit, current_timestamp,
    to_date, datediff, round as spark_round, concat, sha2, regexp_replace
)
from pyspark.sql.types import DoubleType, IntegerType, DateType

# COMMAND ----------

CATALOG = spark.conf.get("catalog", "procurement_lakehouse")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Silver Vendors

# COMMAND ----------

@dlt.table(
    name="silver_vendors",
    comment="Cleansed vendor master data with standardized fields",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_vendor_id", "vendor_id IS NOT NULL")
@dlt.expect_or_drop("valid_vendor_name", "vendor_name IS NOT NULL")
@dlt.expect("valid_rating", "performance_rating BETWEEN 0 AND 5")
def silver_vendors():
    return (
        spark.read.table(f"{CATALOG}.procurement_bronze.bronze_vendors")
        .withColumn("vendor_name", trim(col("vendor_name")))
        .withColumn("country", upper(trim(col("country"))))
        .withColumn("state", upper(trim(col("state"))))
        .withColumn("city", trim(col("city")))
        .withColumn("performance_rating", col("performance_rating").cast(DoubleType()))
        .withColumn("credit_limit_usd", col("credit_limit_usd").cast(DoubleType()))
        .withColumn("annual_turnover_usd", col("annual_turnover_usd").cast(DoubleType()))
        .withColumn("payment_terms_days", col("payment_terms_days").cast(IntegerType()))
        .withColumn("_silver_timestamp", current_timestamp())
        .drop("_ingest_timestamp", "_source_file")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Silver Projects

# COMMAND ----------

@dlt.table(
    name="silver_projects",
    comment="Cleansed project data with derived fields",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_project_id", "project_id IS NOT NULL")
@dlt.expect("valid_budget", "approved_budget_usd > 0")
def silver_projects():
    return (
        spark.read.table(f"{CATALOG}.procurement_bronze.bronze_projects")
        .withColumn("project_name", trim(col("project_name")))
        .withColumn("country", upper(trim(col("country"))))
        .withColumn("contract_value_usd", col("contract_value_usd").cast(DoubleType()))
        .withColumn("approved_budget_usd", col("approved_budget_usd").cast(DoubleType()))
        .withColumn("start_date", to_date(col("start_date")))
        .withColumn("planned_completion_date", to_date(col("planned_completion_date")))
        .withColumn("actual_completion_date", to_date(col("actual_completion_date")))
        .withColumn("planned_duration_days",
            datediff(col("planned_completion_date"), col("start_date")))
        .withColumn("budget_variance_pct",
            when(col("approved_budget_usd") > 0,
                spark_round((col("contract_value_usd") - col("approved_budget_usd"))
                    / col("approved_budget_usd") * 100, 2))
            .otherwise(lit(0.0)))
        .withColumn("_silver_timestamp", current_timestamp())
        .drop("_ingest_timestamp", "_source_file")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Silver Materials

# COMMAND ----------

@dlt.table(
    name="silver_materials",
    comment="Cleansed material catalog with standardized categories",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_material_id", "material_id IS NOT NULL")
@dlt.expect("valid_price", "unit_price >= 0")
def silver_materials():
    return (
        spark.read.table(f"{CATALOG}.procurement_bronze.bronze_materials")
        .withColumn("material_name", trim(col("material_name")))
        .withColumn("category", upper(trim(col("category"))))
        .withColumn("sub_category", trim(col("sub_category")))
        .withColumn("unit_price", col("unit_price").cast(DoubleType()))
        .withColumn("weight_kg", col("weight_kg").cast(DoubleType()))
        .withColumn("lead_time_days", col("lead_time_days").cast(IntegerType()))
        .withColumn("is_active", col("active").cast("boolean"))
        .withColumn("_silver_timestamp", current_timestamp())
        .drop("_ingest_timestamp", "_source_file", "active")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Silver Employees

# COMMAND ----------

@dlt.table(
    name="silver_employees",
    comment="Cleansed employee data",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_employee_id", "employee_id IS NOT NULL")
def silver_employees():
    return (
        spark.read.table(f"{CATALOG}.procurement_bronze.bronze_employees")
        .withColumn("full_name", concat(trim(col("first_name")), lit(" "), trim(col("last_name"))))
        .withColumn("department", upper(trim(col("department"))))
        .withColumn("is_active", col("active").cast("boolean"))
        .withColumn("approval_limit", col("approval_limit").cast(DoubleType()))
        .withColumn("hire_date", to_date(col("hire_date")))
        .withColumn("_silver_timestamp", current_timestamp())
        .drop("_ingest_timestamp", "_source_file", "active")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Silver Purchase Orders

# COMMAND ----------

@dlt.table(
    name="silver_purchase_orders",
    comment="Cleansed purchase order data with enrichments",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_po_id", "po_id IS NOT NULL")
@dlt.expect_or_drop("valid_vendor_ref", "vendor_id IS NOT NULL")
@dlt.expect("valid_po_value", "po_value >= 0")
def silver_purchase_orders():
    return (
        spark.read.table(f"{CATALOG}.procurement_bronze.bronze_purchase_orders")
        .withColumn("po_date", to_date(col("po_date")))
        .withColumn("delivery_date", to_date(col("delivery_date")))
        .withColumn("approval_date", to_date(col("approval_date")))
        .withColumn("po_value", col("po_value").cast(DoubleType()))
        .withColumn("tax_amount", col("tax_amount").cast(DoubleType()))
        .withColumn("total_value", col("total_value").cast(DoubleType()))
        .withColumn("po_status", upper(trim(col("po_status"))))
        .withColumn("order_date", to_date(col("po_date")))
        .withColumn("_silver_timestamp", current_timestamp())
        .drop("_ingest_timestamp", "_source_file")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Silver PO Line Items

# COMMAND ----------

@dlt.table(
    name="silver_po_line_items",
    comment="Cleansed PO line items with calculated fields",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_line_id", "line_id IS NOT NULL")
@dlt.expect_or_drop("valid_po_ref", "po_id IS NOT NULL")
def silver_po_line_items():
    return (
        spark.read.table(f"{CATALOG}.procurement_bronze.bronze_po_line_items")
        .withColumn("qty_ordered", col("qty_ordered").cast(DoubleType()))
        .withColumn("qty_received", col("qty_received").cast(DoubleType()))
        .withColumn("qty_accepted", col("qty_accepted").cast(DoubleType()))
        .withColumn("unit_price", col("unit_price").cast(DoubleType()))
        .withColumn("line_value", col("line_value").cast(DoubleType()))
        .withColumn("delivery_date", to_date(col("delivery_date")))
        .withColumn("actual_delivery_date", to_date(col("actual_delivery_date")))
        .withColumn("fulfillment_rate",
            when(col("qty_ordered") > 0,
                spark_round(col("qty_received") / col("qty_ordered") * 100, 2))
            .otherwise(lit(0.0)))
        .withColumn("acceptance_rate",
            when(col("qty_received") > 0,
                spark_round(col("qty_accepted") / col("qty_received") * 100, 2))
            .otherwise(lit(0.0)))
        .withColumn("_silver_timestamp", current_timestamp())
        .drop("_ingest_timestamp", "_source_file")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Silver Contracts

# COMMAND ----------

@dlt.table(
    name="silver_contracts",
    comment="Cleansed contract data",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_contract_id", "contract_id IS NOT NULL")
@dlt.expect("valid_value", "contract_value > 0")
def silver_contracts():
    return (
        spark.read.table(f"{CATALOG}.procurement_bronze.bronze_contracts")
        .withColumn("contract_value", col("contract_value").cast(DoubleType()))
        .withColumn("start_date", to_date(col("start_date")))
        .withColumn("end_date", to_date(col("end_date")))
        .withColumn("contract_status", upper(trim(col("contract_status"))))
        .withColumn("contract_type", upper(trim(col("contract_type"))))
        .withColumn("contract_duration_days",
            datediff(col("end_date"), col("start_date")))
        .withColumn("_silver_timestamp", current_timestamp())
        .drop("_ingest_timestamp", "_source_file")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Silver Contract Items

# COMMAND ----------

@dlt.table(
    name="silver_contract_items",
    comment="Cleansed contract line items",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_item_id", "item_id IS NOT NULL")
@dlt.expect_or_drop("valid_contract_ref", "contract_id IS NOT NULL")
def silver_contract_items():
    return (
        spark.read.table(f"{CATALOG}.procurement_bronze.bronze_contract_items")
        .withColumn("quantity", col("quantity").cast(DoubleType()))
        .withColumn("unit_price", col("unit_price").cast(DoubleType()))
        .withColumn("line_value", col("line_value").cast(DoubleType()))
        .withColumn("_silver_timestamp", current_timestamp())
        .drop("_ingest_timestamp", "_source_file")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Silver Invoices

# COMMAND ----------

@dlt.table(
    name="silver_invoices",
    comment="Cleansed invoice data with payment analysis",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_invoice_id", "invoice_id IS NOT NULL")
@dlt.expect("valid_amount", "invoice_amount > 0")
def silver_invoices():
    return (
        spark.read.table(f"{CATALOG}.procurement_bronze.bronze_invoices")
        .withColumn("invoice_date", to_date(col("invoice_date")))
        .withColumn("due_date", to_date(col("due_date")))
        .withColumn("payment_date", to_date(col("payment_date")))
        .withColumn("invoice_amount", col("invoice_amount").cast(DoubleType()))
        .withColumn("tax_amount", col("tax_amount").cast(DoubleType()))
        .withColumn("total_amount", col("total_amount").cast(DoubleType()))
        .withColumn("paid_amount", col("paid_amount").cast(DoubleType()))
        .withColumn("invoice_status", upper(trim(col("invoice_status"))))
        .withColumn("days_to_pay",
            when(col("payment_date").isNotNull(),
                datediff(col("payment_date"), col("invoice_date")))
            .otherwise(lit(None)))
        .withColumn("days_overdue",
            when((col("payment_date").isNull()) & (col("due_date").isNotNull()),
                datediff(current_timestamp(), col("due_date")))
            .otherwise(lit(0)))
        .withColumn("_silver_timestamp", current_timestamp())
        .drop("_ingest_timestamp", "_source_file")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Silver Goods Receipts

# COMMAND ----------

@dlt.table(
    name="silver_goods_receipts",
    comment="Cleansed goods receipt data",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_grn_id", "grn_id IS NOT NULL")
@dlt.expect_or_drop("valid_po_ref", "po_id IS NOT NULL")
def silver_goods_receipts():
    return (
        spark.read.table(f"{CATALOG}.procurement_bronze.bronze_goods_receipts")
        .withColumn("receipt_date", to_date(col("receipt_date")))
        .withColumn("qty_received", col("qty_delivered").cast(DoubleType()))
        .withColumn("qty_accepted", col("qty_accepted").cast(DoubleType()))
        .withColumn("qty_rejected", col("qty_rejected").cast(DoubleType()))
        .withColumn("rejection_rate",
            when(col("qty_delivered") > 0,
                spark_round(col("qty_rejected") / col("qty_delivered") * 100, 2))
            .otherwise(lit(0.0)))
        .withColumn("grn_status", upper(trim(col("grn_status"))))
        .withColumn("storage_location", trim(col("storage_location")))
        .withColumn("_silver_timestamp", current_timestamp())
        .drop("_ingest_timestamp", "_source_file")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Silver Project Budgets

# COMMAND ----------

@dlt.table(
    name="silver_project_budgets",
    comment="Cleansed project budget allocations",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_budget_id", "budget_id IS NOT NULL")
@dlt.expect("valid_budget_amount", "revised_amount >= 0")
def silver_project_budgets():
    return (
        spark.read.table(f"{CATALOG}.procurement_bronze.bronze_project_budgets")
        .withColumn("budget_amount", col("revised_amount").cast(DoubleType()))
        .withColumn("original_budget", col("original_amount").cast(DoubleType()))
        .withColumn("committed_amount", col("committed_amount").cast(DoubleType()))
        .withColumn("spent_amount", col("actual_spent").cast(DoubleType()))
        .withColumn("remaining_amount",
            col("revised_amount").cast(DoubleType()) - col("actual_spent").cast(DoubleType()))
        .withColumn("utilization_pct",
            when(col("revised_amount") > 0,
                spark_round(col("actual_spent").cast(DoubleType()) / col("revised_amount").cast(DoubleType()) * 100, 2))
            .otherwise(lit(0.0)))
        .withColumn("wbs_code", trim(col("wbs_code")))
        .withColumn("cost_type", upper(trim(col("cost_type"))))
        .withColumn("budget_status", upper(trim(col("budget_status"))))
        .withColumn("period_start", to_date(col("period_start")))
        .withColumn("period_end", to_date(col("period_end")))
        .withColumn("_silver_timestamp", current_timestamp())
        .drop("_ingest_timestamp", "_source_file")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Silver Project Actuals

# COMMAND ----------

@dlt.table(
    name="silver_project_actuals",
    comment="Cleansed project actual transactions",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_transaction_id", "transaction_id IS NOT NULL")
@dlt.expect("valid_amount", "actual_amount > 0")
def silver_project_actuals():
    return (
        spark.read.table(f"{CATALOG}.procurement_bronze.bronze_project_actuals")
        .withColumn("transaction_date", to_date(col("transaction_date")))
        .withColumn("posting_date", to_date(col("posting_date")))
        .withColumn("amount", col("actual_amount").cast(DoubleType()))
        .withColumn("actual_amount", col("actual_amount").cast(DoubleType()))
        .withColumn("budget_amount", col("budget_amount").cast(DoubleType()))
        .withColumn("variance", col("variance").cast(DoubleType()))
        .withColumn("cost_category", upper(trim(col("cost_type"))))
        .withColumn("cost_type", upper(trim(col("cost_type"))))
        .withColumn("cost_status", upper(trim(col("cost_status"))))
        .withColumn("wbs_code", trim(col("wbs_code")))
        .withColumn("fiscal_period", trim(col("fiscal_period")))
        .withColumn("_silver_timestamp", current_timestamp())
        .drop("_ingest_timestamp", "_source_file")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Silver Sales Orders

# COMMAND ----------

@dlt.table(
    name="silver_sales_orders",
    comment="Cleansed sales order data",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_sales_order_id", "sales_order_id IS NOT NULL")
@dlt.expect("valid_order_value", "revenue > 0")
def silver_sales_orders():
    return (
        spark.read.table(f"{CATALOG}.procurement_bronze.bronze_sales_orders")
        .withColumn("order_date", to_date(col("order_date")))
        .withColumn("delivery_date", to_date(col("delivery_date")))
        .withColumn("order_value", col("revenue").cast(DoubleType()))
        .withColumn("revenue", col("revenue").cast(DoubleType()))
        .withColumn("tax_amount", col("tax_amount").cast(DoubleType()))
        .withColumn("total_value", col("total_amount").cast(DoubleType()))
        .withColumn("order_status", upper(trim(col("order_status"))))
        .withColumn("payment_status", upper(trim(col("payment_status"))))
        .withColumn("order_type", upper(trim(col("order_type"))))
        .withColumn("customer_name", trim(col("customer_name")))
        .withColumn("customer_type", upper(trim(col("customer_type"))))
        .withColumn("quantity", col("quantity").cast(DoubleType()))
        .withColumn("unit_price", col("unit_price").cast(DoubleType()))
        .withColumn("cogs", col("cogs").cast(DoubleType()))
        .withColumn("gross_margin", col("gross_margin").cast(DoubleType()))
        .withColumn("margin_pct", col("margin_pct").cast(DoubleType()))
        .withColumn("_silver_timestamp", current_timestamp())
        .drop("_ingest_timestamp", "_source_file")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. Silver Inventory

# COMMAND ----------

@dlt.table(
    name="silver_inventory",
    comment="Cleansed inventory movement data",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_movement_id", "movement_id IS NOT NULL")
def silver_inventory():
    return (
        spark.read.table(f"{CATALOG}.procurement_bronze.bronze_inventory")
        .withColumn("movement_date", to_date(col("movement_date")))
        .withColumn("quantity", col("quantity").cast(DoubleType()))
        .withColumn("unit_cost", col("unit_cost").cast(DoubleType()))
        .withColumn("total_cost", col("total_value").cast(DoubleType()))
        .withColumn("movement_type", upper(trim(col("movement_type"))))
        .withColumn("from_location", trim(col("from_location")))
        .withColumn("to_location", trim(col("to_location")))
        .withColumn("unit_of_measure", upper(trim(col("unit_of_measure"))))
        .withColumn("reason_code", upper(trim(col("reason_code"))))
        .withColumn("_silver_timestamp", current_timestamp())
        .drop("_ingest_timestamp", "_source_file")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 15. Silver Vendor Performance

# COMMAND ----------

@dlt.table(
    name="silver_vendor_performance",
    comment="Cleansed vendor performance metrics",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_performance_id", "performance_id IS NOT NULL")
@dlt.expect_or_drop("valid_vendor_ref", "vendor_id IS NOT NULL")
def silver_vendor_performance():
    return (
        spark.read.table(f"{CATALOG}.procurement_bronze.bronze_vendor_performance")
        .withColumn("evaluation_date", to_date(col("evaluation_date")))
        .withColumn("delivery_score", col("delivery_score").cast(DoubleType()))
        .withColumn("quality_score", col("quality_score").cast(DoubleType()))
        .withColumn("commercial_score", col("commercial_score").cast(DoubleType()))
        .withColumn("hse_score", col("hse_score").cast(DoubleType()))
        .withColumn("compliance_score", col("compliance_score").cast(DoubleType()))
        .withColumn("overall_score", col("composite_score").cast(DoubleType()))
        .withColumn("recommendation", trim(col("recommendation")))
        .withColumn("evaluation_period", upper(trim(col("evaluation_period"))))
        .withColumn("_silver_timestamp", current_timestamp())
        .drop("_ingest_timestamp", "_source_file", "composite_score")
    )
