# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - Raw Data Ingestion
# MAGIC Construction & Oil/Gas Procurement Lakehouse

# COMMAND ----------

import dlt
from pyspark.sql.functions import current_timestamp, input_file_name, lit

# COMMAND ----------

# Configuration
CATALOG = spark.conf.get("catalog", "procurement_lakehouse")
SCHEMA_BRONZE = "bronze"
RAW_PATH = spark.conf.get("raw_data_path", "/mnt/raw/procurement")

# COMMAND ----------

# Source table definitions - one per CSV file
TABLES = [
    "vendors", "projects", "materials", "employees",
    "purchase_orders", "po_line_items", "contracts", "contract_items",
    "invoices", "goods_receipts", "project_budgets", "project_actuals",
    "sales_orders", "inventory", "vendor_performance"
]

# COMMAND ----------

def create_bronze_table(table_name):
    @dlt.table(
        name=f"bronze_{table_name}",
        comment=f"Raw {table_name} data ingested from CSV",
        table_properties={"quality": "bronze", "pipelines.autoOptimize.managed": "true"}
    )
    def bronze_table():
        return (
            spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(f"{RAW_PATH}/{table_name}.csv")
            .withColumn("_ingest_timestamp", current_timestamp())
            .withColumn("_source_file", lit(f"{table_name}.csv"))
        )
    return bronze_table

# COMMAND ----------

# Register all 15 bronze tables
for t in TABLES:
    create_bronze_table(t)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Expectations (Bronze)
# MAGIC Bronze layer performs minimal validation - just ensuring data was loaded.

# COMMAND ----------

@dlt.table(
    name="bronze_load_audit",
    comment="Audit log tracking bronze data loads"
)
def bronze_load_audit():
    from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType
    from pyspark.sql import Row
    import datetime

    rows = []
    for t in TABLES:
        try:
            df = spark.read.table(f"{CATALOG}.{SCHEMA_BRONZE}.bronze_{t}")
            cnt = df.count()
            rows.append(Row(
                table_name=t,
                record_count=cnt,
                load_timestamp=datetime.datetime.now(),
                status="SUCCESS"
            ))
        except Exception as e:
            rows.append(Row(
                table_name=t,
                record_count=0,
                load_timestamp=datetime.datetime.now(),
                status=f"FAILED: {str(e)[:200]}"
            ))
    return spark.createDataFrame(rows)
