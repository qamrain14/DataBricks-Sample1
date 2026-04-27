import pytest  
from pyspark.sql import SparkSession  
  
  
@pytest.fixture(scope="session")  
def spark():  
    return (  
        SparkSession.builder.master("local[*]")  
        .appName("pytest")  
        .config("spark.sql.shuffle.partitions", "1")  
        .getOrCreate()  
    )  
  
  
@pytest.fixture(scope="session")  
def bronze_data(spark):  
    tables = ["vendors","projects","materials","employees","purchase_orders","po_line_items","contracts","contract_items","invoices","goods_receipts","project_budgets","project_actuals","sales_orders","inventory","vendor_performance"]  
    return {t: spark.createDataFrame([], schema="id STRING") for t in tables}  
  
  
@pytest.fixture(scope="session")  
def register_all_bronze(spark, bronze_data):  
    for name, df in bronze_data.items():  
        df.createOrReplaceTempView(f"bronze_{name}")  
    return bronze_data  
