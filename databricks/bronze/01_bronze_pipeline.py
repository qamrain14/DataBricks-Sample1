# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - Raw Data Ingestion
# MAGIC Construction & Oil/Gas Procurement Lakehouse
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC import dlt
# MAGIC from pyspark.sql.functions import current_timestamp, input_file_name, lit
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC # Configuration
# MAGIC CATALOG = spark.conf.get("catalog", "procurement_lakehouse")
# MAGIC SCHEMA_BRONZE = "bronze"
# MAGIC RAW_PATH = spark.conf.get("raw_data_path", "/mnt/raw/procurement")
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC # Source table definitions - one per Excel file
# MAGIC TABLES = [
# MAGIC     "vendors", "projects", "materials", "employees",
# MAGIC     "purchase_orders", "po_line_items", "contracts", "contract_items",
# MAGIC     "invoices", "goods_receipts", "project_budgets", "project_actuals",
# MAGIC     "sales_orders", "inventory", "vendor_performance"
# MAGIC ]
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC def create_bronze_table(table_name):
# MAGIC     @dlt.table(
# MAGIC         name=f"bronze_{table_name}",
# MAGIC         comment=f"Raw {table_name} data ingested from Excel",
# MAGIC         table_properties={"quality": "bronze", "pipelines.autoOptimize.managed": "true"}
# MAGIC     )
# MAGIC     def bronze_table():
# MAGIC         return (
# MAGIC                         spark.read.format("csv")
# MAGIC             .option("header", "true")
# MAGIC             .option("inferSchema", "true")
# MAGIC             .load(f"{RAW_PATH}/{table_name}.csv")
# MAGIC             .withColumn("_ingest_timestamp", current_timestamp())
# MAGIC             .withColumn("_source_file", lit(f"{table_name}.csv"))
# MAGIC         )
# MAGIC     return bronze_table
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC # Register all 15 bronze tables
# MAGIC for t in TABLES:
# MAGIC     create_bronze_table(t)
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC %md
# MAGIC ## Data Quality Expectations (Bronze)
# MAGIC Bronze layer performs minimal validation - just ensuring data was loaded.
# MAGIC 
# MAGIC # COMMAND ----------
# MAGIC 
# MAGIC @dlt.table(
# MAGIC     name="bronze_load_audit",
# MAGIC     comment="Audit log tracking bronze data loads"
# MAGIC )
# MAGIC def bronze_load_audit():
# MAGIC     from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType
# MAGIC     from pyspark.sql import Row
# MAGIC     import datetime
# MAGIC 
# MAGIC     rows = []
# MAGIC     for t in TABLES:
# MAGIC         try:
# MAGIC             df = spark.read.table(f"{CATALOG}.{SCHEMA_BRONZE}.bronze_{t}")
# MAGIC             cnt = df.count()
# MAGIC             rows.append(Row(
# MAGIC                 table_name=t,
# MAGIC                 record_count=cnt,
# MAGIC                 load_timestamp=datetime.datetime.now(),
# MAGIC                 status="SUCCESS"
# MAGIC             ))
# MAGIC         except Exception as e:
# MAGIC             rows.append(Row(
# MAGIC                 table_name=t,
# MAGIC                 record_count=0,
# MAGIC                 load_timestamp=datetime.datetime.now(),
# MAGIC                 status=f"FAILED: {str(e)[:200]}"
# MAGIC             ))
# MAGIC     return spark.createDataFrame(rows)