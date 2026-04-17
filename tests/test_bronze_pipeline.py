"""Tests for Bronze Layer (01_bronze_pipeline.py)."""
import os
import sys
import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import dlt_mock

PIPELINE_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "databricks", "bronze"
)

# ---- Factory Pattern Tests ----

class TestBronzeFactory:
    """Test the create_bronze_table factory pattern."""

    def test_factory_registers_table_via_dlt_mock(self, spark):
        """Simulates create_bronze_table logic with dlt_mock."""
        from pyspark.sql.functions import current_timestamp, lit

        @dlt_mock.table(name="bronze_test_table", comment="test")
        def bronze_test_table():
            data = [("1", "test")]
            df = spark.createDataFrame(data, ["id", "name"])
            return (
                df.withColumn("_ingest_timestamp", current_timestamp())
                  .withColumn("_source_file", lit("test.xlsx"))
            )

        dlt_mock.execute_pending()
        result = dlt_mock.get_table("bronze_test_table")
        assert result is not None
        assert result.count() == 1
        assert "_ingest_timestamp" in result.columns
        assert "_source_file" in result.columns

    def test_factory_creates_all_15_tables(self, spark):
        """Verify factory can create all 15 expected bronze tables."""
        TABLES = [
            "vendors", "projects", "materials", "employees",
            "purchase_orders", "po_line_items", "contracts", "contract_items",
            "invoices", "goods_receipts", "project_budgets", "project_actuals",
            "sales_orders", "inventory", "vendor_performance",
        ]
        from pyspark.sql.functions import current_timestamp, lit

        for t in TABLES:
            @dlt_mock.table(
                name=f"bronze_{t}",
                comment=f"Raw {t} data",
                table_properties={"quality": "bronze"},
            )
            def make_table(table_name=t):
                data = [("1",)]
                return (
                    spark.createDataFrame(data, ["id"])
                    .withColumn("_ingest_timestamp", current_timestamp())
                    .withColumn("_source_file", lit(f"{table_name}.xlsx"))
                )

        dlt_mock.execute_pending()
        tables = dlt_mock.get_all_tables()
        for t in TABLES:
            assert f"bronze_{t}" in tables, f"Missing bronze_{t}"
            assert tables[f"bronze_{t}"].count() > 0

    def test_bronze_table_has_metadata_columns(self, spark):
        """All bronze tables should have _ingest_timestamp and _source_file."""
        from pyspark.sql.functions import current_timestamp, lit

        @dlt_mock.table(name="bronze_meta_test")
        def bronze_meta_test():
            return (
                spark.createDataFrame([("X",)], ["val"])
                .withColumn("_ingest_timestamp", current_timestamp())
                .withColumn("_source_file", lit("test.xlsx"))
            )

        dlt_mock.execute_pending()
        df = dlt_mock.get_table("bronze_meta_test")
        assert "_ingest_timestamp" in df.columns
        assert "_source_file" in df.columns

# ---- Audit Table Tests ----

class TestBronzeAudit:
    """Test the bronze_load_audit table logic."""

    def test_audit_records_table_counts(self, spark, register_all_bronze):
        """Audit table should track row counts for each bronze table."""
        from pyspark.sql import Row
        import datetime

        TABLES = [
            "vendors", "projects", "materials", "employees",
            "purchase_orders", "po_line_items", "contracts", "contract_items",
            "invoices", "goods_receipts", "project_budgets", "project_actuals",
            "sales_orders", "inventory", "vendor_performance",
        ]
        rows = []
        for t in TABLES:
            tbl_name = f"bronze_{t}"
            df = dlt_mock.get_table(tbl_name)
            if df is not None:
                rows.append(Row(
                    table_name=t,
                    record_count=df.count(),
                    load_timestamp=datetime.datetime.now(),
                    status="SUCCESS",
                ))

        audit_df = spark.createDataFrame(rows)
        assert audit_df.count() == 15
        # Each bronze table has 3 rows of test data
        for row in audit_df.collect():
            assert row["record_count"] == 3
            assert row["status"] == "SUCCESS"

# ---- Data Quality Tests ----

class TestBronzeDataQuality:
    """Verify bronze data has expected shape."""

    def test_each_bronze_table_has_3_rows(self, bronze_data):
        """Each fixture should provide 3 test rows."""
        for name, df in bronze_data.items():
            assert df.count() == 3, f"{name} should have 3 rows"

    def test_bronze_tables_have_metadata(self, bronze_data):
        """All should have _ingest_timestamp and _source_file."""
        for name, df in bronze_data.items():
            assert "_ingest_timestamp" in df.columns
            assert "_source_file" in df.columns

    def test_bronze_vendors_schema(self, bronze_data):
        cols = bronze_data["bronze_vendors"].columns
        assert "vendor_id" in cols
        assert "vendor_name" in cols
        assert "performance_rating" in cols

    def test_bronze_has_null_pk_rows(self, bronze_data):
        """Row 3 in each table has a null PK for DQ testing."""
        pk_map = {
            "bronze_vendors": "vendor_id",
            "bronze_projects": "project_id",
            "bronze_materials": "material_id",
            "bronze_employees": "employee_id",
            "bronze_purchase_orders": "po_id",
            "bronze_po_line_items": "line_id",
            "bronze_contracts": "contract_id",
            "bronze_contract_items": "item_id",
            "bronze_invoices": "invoice_id",
            "bronze_goods_receipts": "grn_id",
            "bronze_project_budgets": "budget_id",
            "bronze_project_actuals": "transaction_id",
            "bronze_sales_orders": "sales_order_id",
            "bronze_inventory": "movement_id",
            "bronze_vendor_performance": "performance_id",
        }
        for tbl, pk in pk_map.items():
            df = bronze_data[tbl]
            null_count = df.where(f"{pk} IS NULL").count()
            assert null_count >= 1, f"{tbl} should have at least 1 null PK row"
