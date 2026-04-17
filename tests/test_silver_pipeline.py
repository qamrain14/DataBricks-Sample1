"""Tests for Silver Layer (02_silver_pipeline.py)."""
import os
import sys
import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import dlt_mock
import pipeline_runner

PIPELINE_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "databricks", "silver", "02_silver_pipeline.py"
)


@pytest.fixture
def silver_tables(spark, register_all_bronze):
    """Execute silver pipeline and return all silver tables."""
    tables = pipeline_runner.run_pipeline(PIPELINE_PATH, spark, dlt_mock)
    return tables


# ---- Pipeline Execution Tests ----

class TestSilverPipelineExecution:
    """Verify silver pipeline executes and produces all 15 tables."""

    def test_silver_pipeline_creates_15_tables(self, silver_tables):
        expected = [
            "silver_vendors", "silver_projects", "silver_materials",
            "silver_employees", "silver_purchase_orders", "silver_po_line_items",
            "silver_contracts", "silver_contract_items", "silver_invoices",
            "silver_goods_receipts", "silver_project_budgets",
            "silver_project_actuals", "silver_sales_orders",
            "silver_inventory", "silver_vendor_performance",
        ]
        for t in expected:
            assert t in silver_tables, f"Missing table: {t}"

    def test_silver_tables_not_empty(self, silver_tables):
        for name, df in silver_tables.items():
            if name.startswith("silver_"):
                assert df.count() > 0, f"{name} is empty"


# ---- DQ Expectation Tests (expect_or_drop removes null PKs) ----

class TestSilverDataQuality:
    """Verify expect_or_drop removes null PK rows."""

    def test_vendors_null_pk_dropped(self, silver_tables):
        df = silver_tables["silver_vendors"]
        null_count = df.where("vendor_id IS NULL").count()
        assert null_count == 0

    def test_projects_null_pk_dropped(self, silver_tables):
        df = silver_tables["silver_projects"]
        assert df.where("project_id IS NULL").count() == 0

    def test_materials_null_pk_dropped(self, silver_tables):
        df = silver_tables["silver_materials"]
        assert df.where("material_id IS NULL").count() == 0

    def test_employees_null_pk_dropped(self, silver_tables):
        df = silver_tables["silver_employees"]
        assert df.where("employee_id IS NULL").count() == 0

    def test_purchase_orders_null_pk_dropped(self, silver_tables):
        df = silver_tables["silver_purchase_orders"]
        assert df.where("po_id IS NULL").count() == 0

    def test_po_line_items_null_pk_dropped(self, silver_tables):
        df = silver_tables["silver_po_line_items"]
        assert df.where("line_id IS NULL").count() == 0

    def test_contracts_null_pk_dropped(self, silver_tables):
        df = silver_tables["silver_contracts"]
        assert df.where("contract_id IS NULL").count() == 0

    def test_contract_items_null_pk_dropped(self, silver_tables):
        df = silver_tables["silver_contract_items"]
        assert df.where("item_id IS NULL").count() == 0

    def test_invoices_null_pk_dropped(self, silver_tables):
        df = silver_tables["silver_invoices"]
        assert df.where("invoice_id IS NULL").count() == 0

    def test_goods_receipts_null_pk_dropped(self, silver_tables):
        df = silver_tables["silver_goods_receipts"]
        assert df.where("grn_id IS NULL").count() == 0

    def test_project_budgets_null_pk_dropped(self, silver_tables):
        df = silver_tables["silver_project_budgets"]
        assert df.where("budget_id IS NULL").count() == 0

    def test_project_actuals_null_pk_dropped(self, silver_tables):
        df = silver_tables["silver_project_actuals"]
        assert df.where("transaction_id IS NULL").count() == 0

    def test_sales_orders_null_pk_dropped(self, silver_tables):
        df = silver_tables["silver_sales_orders"]
        assert df.where("sales_order_id IS NULL").count() == 0

    def test_inventory_null_pk_dropped(self, silver_tables):
        df = silver_tables["silver_inventory"]
        assert df.where("movement_id IS NULL").count() == 0

    def test_vendor_performance_null_pk_dropped(self, silver_tables):
        df = silver_tables["silver_vendor_performance"]
        assert df.where("performance_id IS NULL").count() == 0


# ---- Row Count Tests (3 input rows, 1 null PK → 2 valid rows) ----

class TestSilverRowCounts:

    @pytest.mark.parametrize("table_name", [
        "silver_vendors", "silver_projects", "silver_materials",
        "silver_employees", "silver_purchase_orders", "silver_po_line_items",
        "silver_contracts", "silver_contract_items", "silver_invoices",
        "silver_goods_receipts", "silver_project_budgets",
        "silver_project_actuals", "silver_sales_orders",
        "silver_inventory", "silver_vendor_performance",
    ])
    def test_silver_table_has_2_rows(self, silver_tables, table_name):
        """After expect_or_drop on null PK, each table should have 2 rows."""
        df = silver_tables[table_name]
        assert df.count() == 2, f"{table_name} expected 2 rows, got {df.count()}"


# ---- Transform Tests ----

class TestSilverVendorTransforms:

    def test_vendor_name_trimmed(self, silver_tables):
        df = silver_tables["silver_vendors"]
        names = [r["vendor_name"] for r in df.collect()]
        for name in names:
            assert name == name.strip(), f"vendor_name not trimmed: '{name}'"

    def test_country_uppercased(self, silver_tables):
        df = silver_tables["silver_vendors"]
        for row in df.collect():
            assert row["country"] == row["country"].upper()

    def test_performance_rating_is_double(self, silver_tables):
        df = silver_tables["silver_vendors"]
        from pyspark.sql.types import DoubleType
        field = [f for f in df.schema.fields if f.name == "performance_rating"][0]
        assert field.dataType == DoubleType()

    def test_payment_terms_is_integer(self, silver_tables):
        df = silver_tables["silver_vendors"]
        from pyspark.sql.types import IntegerType
        field = [f for f in df.schema.fields if f.name == "payment_terms_days"][0]
        assert field.dataType == IntegerType()

    def test_metadata_columns_dropped(self, silver_tables):
        df = silver_tables["silver_vendors"]
        assert "_ingest_timestamp" not in df.columns
        assert "_source_file" not in df.columns

    def test_silver_timestamp_added(self, silver_tables):
        df = silver_tables["silver_vendors"]
        assert "_silver_timestamp" in df.columns


class TestSilverProjectTransforms:

    def test_project_name_trimmed(self, silver_tables):
        df = silver_tables["silver_projects"]
        for row in df.collect():
            assert row["project_name"] == row["project_name"].strip()

    def test_dates_cast_to_date_type(self, silver_tables):
        df = silver_tables["silver_projects"]
        from pyspark.sql.types import DateType
        for col_name in ["start_date", "planned_completion_date"]:
            field = [f for f in df.schema.fields if f.name == col_name][0]
            assert field.dataType == DateType(), f"{col_name} should be DateType"

    def test_planned_duration_days_calculated(self, silver_tables):
        df = silver_tables["silver_projects"]
        assert "planned_duration_days" in df.columns
        row = df.where("project_id = 'P001'").collect()[0]
        assert row["planned_duration_days"] is not None
        assert row["planned_duration_days"] > 0

    def test_budget_variance_pct_calculated(self, silver_tables):
        df = silver_tables["silver_projects"]
        assert "budget_variance_pct" in df.columns


class TestSilverMaterialTransforms:

    def test_category_uppercased(self, silver_tables):
        df = silver_tables["silver_materials"]
        for row in df.collect():
            assert row["category"] == row["category"].upper()

    def test_active_to_is_active_boolean(self, silver_tables):
        df = silver_tables["silver_materials"]
        assert "is_active" in df.columns
        assert "active" not in df.columns


class TestSilverEmployeeTransforms:

    def test_full_name_concatenated(self, silver_tables):
        df = silver_tables["silver_employees"]
        assert "full_name" in df.columns
        row = df.where("employee_id = 'E001'").collect()[0]
        assert "John" in row["full_name"]
        assert "Smith" in row["full_name"]

    def test_full_name_trimmed_parts(self, silver_tables):
        df = silver_tables["silver_employees"]
        row = df.where("employee_id = 'E002'").collect()[0]
        # " Jane " + " " + " Doe " → trim each → "Jane Doe"
        assert row["full_name"] == "Jane Doe"

    def test_department_uppercased(self, silver_tables):
        df = silver_tables["silver_employees"]
        for row in df.collect():
            assert row["department"] == row["department"].upper()


class TestSilverPurchaseOrderTransforms:

    def test_po_status_uppercased(self, silver_tables):
        df = silver_tables["silver_purchase_orders"]
        for row in df.collect():
            val = row["po_status"]
            assert val == val.strip().upper()

    def test_dates_cast(self, silver_tables):
        df = silver_tables["silver_purchase_orders"]
        from pyspark.sql.types import DateType
        field = [f for f in df.schema.fields if f.name == "po_date"][0]
        assert field.dataType == DateType()

    def test_order_date_alias(self, silver_tables):
        df = silver_tables["silver_purchase_orders"]
        assert "order_date" in df.columns


class TestSilverPoLineItemTransforms:

    def test_fulfillment_rate_calculated(self, silver_tables):
        df = silver_tables["silver_po_line_items"]
        assert "fulfillment_rate" in df.columns
        row = df.where("line_id = 'LI001'").collect()[0]
        # 95 received / 100 ordered * 100 = 95.0
        assert row["fulfillment_rate"] == 95.0

    def test_acceptance_rate_calculated(self, silver_tables):
        df = silver_tables["silver_po_line_items"]
        assert "acceptance_rate" in df.columns
        row = df.where("line_id = 'LI001'").collect()[0]
        # 90 accepted / 95 received * 100 ≈ 94.74
        assert abs(row["acceptance_rate"] - 94.74) < 0.1


class TestSilverContractTransforms:

    def test_contract_duration_days(self, silver_tables):
        df = silver_tables["silver_contracts"]
        assert "contract_duration_days" in df.columns
        row = df.where("contract_id = 'C001'").collect()[0]
        assert row["contract_duration_days"] == 365

    def test_status_uppercased(self, silver_tables):
        df = silver_tables["silver_contracts"]
        for row in df.collect():
            assert row["contract_status"] == row["contract_status"].strip().upper()

    def test_type_uppercased(self, silver_tables):
        df = silver_tables["silver_contracts"]
        for row in df.collect():
            assert row["contract_type"] == row["contract_type"].strip().upper()


class TestSilverContractItemTransforms:

    def test_quantity_cast_to_double(self, silver_tables):
        df = silver_tables["silver_contract_items"]
        from pyspark.sql.types import DoubleType
        field = [f for f in df.schema.fields if f.name == "quantity"][0]
        assert field.dataType == DoubleType()


class TestSilverInvoiceTransforms:

    def test_days_to_pay_calculated(self, silver_tables):
        df = silver_tables["silver_invoices"]
        assert "days_to_pay" in df.columns
        row = df.where("invoice_id = 'INV001'").collect()[0]
        # 2024-02-25 minus 2024-02-01 = 24 days
        assert row["days_to_pay"] == 24

    def test_days_overdue_for_unpaid(self, silver_tables):
        df = silver_tables["silver_invoices"]
        row = df.where("invoice_id = 'INV002'").collect()[0]
        assert row["days_overdue"] is not None
        assert row["days_overdue"] > 0


class TestSilverGoodsReceiptTransforms:

    def test_qty_delivered_renamed_to_qty_received(self, silver_tables):
        df = silver_tables["silver_goods_receipts"]
        assert "qty_received" in df.columns

    def test_rejection_rate_calculated(self, silver_tables):
        df = silver_tables["silver_goods_receipts"]
        assert "rejection_rate" in df.columns
        row = df.where("grn_id = 'GR001'").collect()[0]
        # 5 / 100 * 100 = 5.0
        assert row["rejection_rate"] == 5.0


class TestSilverProjectBudgetTransforms:

    def test_column_renames(self, silver_tables):
        df = silver_tables["silver_project_budgets"]
        assert "budget_amount" in df.columns
        assert "original_budget" in df.columns
        assert "spent_amount" in df.columns

    def test_remaining_amount_calculated(self, silver_tables):
        df = silver_tables["silver_project_budgets"]
        assert "remaining_amount" in df.columns

    def test_utilization_pct_calculated(self, silver_tables):
        df = silver_tables["silver_project_budgets"]
        assert "utilization_pct" in df.columns
        row = df.where("budget_id = 'B001'").collect()[0]
        # 1800000 / 2200000 * 100 ≈ 81.82
        assert abs(row["utilization_pct"] - 81.82) < 0.1


class TestSilverProjectActualTransforms:

    def test_cost_category_derived(self, silver_tables):
        df = silver_tables["silver_project_actuals"]
        assert "cost_category" in df.columns
        row = df.where("transaction_id = 'A001'").collect()[0]
        assert row["cost_category"] == "MATERIALS"

    def test_trimmed_cost_type(self, silver_tables):
        df = silver_tables["silver_project_actuals"]
        row = df.where("transaction_id = 'A002'").collect()[0]
        assert row["cost_type"] == "LABOR"


class TestSilverSalesOrderTransforms:

    def test_order_value_from_revenue(self, silver_tables):
        df = silver_tables["silver_sales_orders"]
        assert "order_value" in df.columns

    def test_total_value_from_total_amount(self, silver_tables):
        df = silver_tables["silver_sales_orders"]
        assert "total_value" in df.columns

    def test_customer_name_trimmed(self, silver_tables):
        df = silver_tables["silver_sales_orders"]
        for row in df.collect():
            assert row["customer_name"] == row["customer_name"].strip()


class TestSilverInventoryTransforms:

    def test_total_cost_from_total_value(self, silver_tables):
        df = silver_tables["silver_inventory"]
        assert "total_cost" in df.columns

    def test_movement_type_uppercased(self, silver_tables):
        df = silver_tables["silver_inventory"]
        for row in df.collect():
            assert row["movement_type"] == row["movement_type"].strip().upper()


class TestSilverVendorPerformanceTransforms:

    def test_overall_score_from_composite(self, silver_tables):
        df = silver_tables["silver_vendor_performance"]
        assert "overall_score" in df.columns
        assert "composite_score" not in df.columns

    def test_overall_score_value(self, silver_tables):
        df = silver_tables["silver_vendor_performance"]
        from pyspark.sql.types import DoubleType
        field = [f for f in df.schema.fields if f.name == "overall_score"][0]
        assert field.dataType == DoubleType()
