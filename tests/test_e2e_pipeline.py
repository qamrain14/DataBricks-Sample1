"""End-to-End pipeline tests: Bronze → Silver → Gold sequential flow."""
import os
import sys
import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import dlt_mock
import pipeline_runner

PIPELINES_ROOT = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "databricks"
)
SILVER_PATH = os.path.join(PIPELINES_ROOT, "silver", "02_silver_pipeline.py")
GOLD_PATH = os.path.join(PIPELINES_ROOT, "gold", "03_gold_pipeline.py")


class TestSequentialDependency:
    """Verify layers must execute in order: Bronze → Silver → Gold."""

    def test_silver_fails_without_bronze(self, spark):
        """Silver pipeline should fail if bronze tables are not registered."""
        with pytest.raises((KeyError, RuntimeError)):
            pipeline_runner.run_pipeline(SILVER_PATH, spark, dlt_mock)

    def test_gold_fails_without_silver(self, spark, register_all_bronze):
        """Gold pipeline should fail if silver tables are not registered."""
        with pytest.raises((KeyError, RuntimeError)):
            pipeline_runner.run_pipeline(GOLD_PATH, spark, dlt_mock)


class TestEndToEndDataPropagation:
    """Verify data flows from Bronze through Silver to Gold."""

    @pytest.fixture(autouse=True)
    def _run_full_pipeline(self, spark, register_all_bronze):
        """Execute the full pipeline chain."""
        pipeline_runner.run_pipeline(SILVER_PATH, spark, dlt_mock)
        pipeline_runner.run_pipeline(GOLD_PATH, spark, dlt_mock)
        self.tables = dlt_mock.get_all_tables()

    def test_bronze_tables_present(self):
        bronze_count = sum(1 for k in self.tables if k.startswith("bronze_"))
        assert bronze_count == 15

    def test_silver_tables_present(self):
        silver_count = sum(1 for k in self.tables if k.startswith("silver_"))
        assert silver_count == 15

    def test_gold_dimensions_present(self):
        dims = [k for k in self.tables if k.startswith("dim_")]
        assert len(dims) == 9

    def test_gold_facts_present(self):
        facts = [k for k in self.tables if k.startswith("fact_")]
        assert len(facts) == 9

    def test_total_table_count(self):
        """15 bronze + 15 silver + 9 dims + 9 facts = 48 tables."""
        total = len(self.tables)
        assert total >= 48, f"Expected >= 48 tables, got {total}"


class TestVendorDataPropagation:
    """Trace vendor V001 from bronze through to gold."""

    @pytest.fixture(autouse=True)
    def _run_full_pipeline(self, spark, register_all_bronze):
        pipeline_runner.run_pipeline(SILVER_PATH, spark, dlt_mock)
        pipeline_runner.run_pipeline(GOLD_PATH, spark, dlt_mock)
        self.tables = dlt_mock.get_all_tables()

    def test_vendor_in_bronze(self):
        df = self.tables["bronze_vendors"]
        assert df.where("vendor_id = 'V001'").count() == 1

    def test_vendor_in_silver(self):
        df = self.tables["silver_vendors"]
        assert df.where("vendor_id = 'V001'").count() == 1

    def test_vendor_in_gold_dim(self):
        df = self.tables["dim_vendor"]
        assert df.where("vendor_id = 'V001'").count() == 1

    def test_vendor_in_gold_fact_performance(self):
        df = self.tables["fact_vendor_performance"]
        assert df.where("vendor_id = 'V001'").count() >= 1

    def test_null_vendor_dropped_in_silver(self):
        df = self.tables["silver_vendors"]
        assert df.where("vendor_id IS NULL").count() == 0

    def test_null_vendor_absent_in_gold(self):
        df = self.tables["dim_vendor"]
        assert df.where("vendor_id IS NULL").count() == 0


class TestPurchaseOrderPropagation:
    """Trace PO001 from bronze through to gold fact."""

    @pytest.fixture(autouse=True)
    def _run_full_pipeline(self, spark, register_all_bronze):
        pipeline_runner.run_pipeline(SILVER_PATH, spark, dlt_mock)
        pipeline_runner.run_pipeline(GOLD_PATH, spark, dlt_mock)
        self.tables = dlt_mock.get_all_tables()

    def test_po_in_bronze(self):
        df = self.tables["bronze_purchase_orders"]
        assert df.where("po_id = 'PO001'").count() == 1

    def test_po_in_silver(self):
        df = self.tables["silver_purchase_orders"]
        assert df.where("po_id = 'PO001'").count() == 1

    def test_po_in_gold_fact(self):
        df = self.tables["fact_purchase_orders"]
        assert df.where("po_id = 'PO001'").count() == 1

    def test_po_line_items_aggregated_in_gold(self):
        df = self.tables["fact_purchase_orders"]
        row = df.where("po_id = 'PO001'").collect()[0]
        # PO001 has 2 line items in bronze
        assert row["line_total"] > 0
        assert row["total_quantity"] > 0


class TestContractPropagation:
    """Trace contract C001 from bronze through to gold."""

    @pytest.fixture(autouse=True)
    def _run_full_pipeline(self, spark, register_all_bronze):
        pipeline_runner.run_pipeline(SILVER_PATH, spark, dlt_mock)
        pipeline_runner.run_pipeline(GOLD_PATH, spark, dlt_mock)
        self.tables = dlt_mock.get_all_tables()

    def test_contract_in_bronze(self):
        df = self.tables["bronze_contracts"]
        assert df.where("contract_id = 'C001'").count() == 1

    def test_contract_in_silver(self):
        df = self.tables["silver_contracts"]
        assert df.where("contract_id = 'C001'").count() == 1

    def test_contract_in_gold_dim(self):
        df = self.tables["dim_contract"]
        assert df.where("contract_id = 'C001'").count() == 1

    def test_contract_in_gold_fact(self):
        df = self.tables["fact_contracts"]
        assert df.where("contract_id = 'C001'").count() == 1

    def test_contract_items_aggregated(self):
        df = self.tables["fact_contracts"]
        row = df.where("contract_id = 'C001'").collect()[0]
        assert row["item_count"] == 2
        assert abs(row["items_total"] - 850000.0) < 1


class TestDataQualityPropagation:
    """Verify DQ rules propagate correctly through layers."""

    @pytest.fixture(autouse=True)
    def _run_full_pipeline(self, spark, register_all_bronze):
        pipeline_runner.run_pipeline(SILVER_PATH, spark, dlt_mock)
        pipeline_runner.run_pipeline(GOLD_PATH, spark, dlt_mock)
        self.tables = dlt_mock.get_all_tables()

    def test_null_pks_removed_at_silver(self):
        """All silver tables should have 0 null PKs."""
        pk_map = {
            "silver_vendors": "vendor_id",
            "silver_projects": "project_id",
            "silver_materials": "material_id",
            "silver_employees": "employee_id",
            "silver_purchase_orders": "po_id",
            "silver_po_line_items": "line_id",
            "silver_contracts": "contract_id",
            "silver_contract_items": "item_id",
            "silver_invoices": "invoice_id",
            "silver_goods_receipts": "grn_id",
            "silver_project_budgets": "budget_id",
            "silver_project_actuals": "transaction_id",
            "silver_sales_orders": "sales_order_id",
            "silver_inventory": "movement_id",
            "silver_vendor_performance": "performance_id",
        }
        for tbl, pk in pk_map.items():
            df = self.tables[tbl]
            assert df.where(f"{pk} IS NULL").count() == 0, \
                f"{tbl} has null {pk} after silver"

    def test_gold_row_counts_consistent(self):
        """Gold dimensions should have <= silver row counts."""
        silver_vendors = self.tables["silver_vendors"].count()
        gold_vendors = self.tables["dim_vendor"].count()
        assert gold_vendors <= silver_vendors

    def test_expectations_recorded(self):
        """dlt_mock should have recorded expectation results."""
        expectations = dlt_mock.get_expectations()
        assert len(expectations) > 0


class TestTransformPersistence:
    """Verify silver transforms persist through to gold."""

    @pytest.fixture(autouse=True)
    def _run_full_pipeline(self, spark, register_all_bronze):
        pipeline_runner.run_pipeline(SILVER_PATH, spark, dlt_mock)
        pipeline_runner.run_pipeline(GOLD_PATH, spark, dlt_mock)
        self.tables = dlt_mock.get_all_tables()

    def test_trimmed_names_in_gold(self):
        """Vendor names trimmed in silver should remain trimmed in gold."""
        df = self.tables["dim_vendor"]
        for row in df.collect():
            name = row["vendor_name"]
            assert name == name.strip(), f"Not trimmed in gold: '{name}'"

    def test_type_casting_persists(self):
        """Double casting in silver should persist in gold."""
        from pyspark.sql.types import DoubleType
        df = self.tables["dim_vendor"]
        field = [f for f in df.schema.fields if f.name == "performance_rating"][0]
        assert field.dataType == DoubleType()

    def test_derived_columns_in_gold(self):
        """Silver-derived columns should be available in gold."""
        df = self.tables["dim_project"]
        assert "budget_variance_pct" in df.columns
