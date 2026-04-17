"""Tests for Gold Layer (03_gold_pipeline.py)."""
import os
import sys
import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import dlt_mock
import pipeline_runner

SILVER_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "databricks", "silver", "02_silver_pipeline.py"
)
GOLD_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "databricks", "gold", "03_gold_pipeline.py"
)


@pytest.fixture
def gold_tables(spark, register_all_bronze):
    """Execute silver then gold pipeline, return all tables."""
    # First run silver to produce silver tables
    pipeline_runner.run_pipeline(SILVER_PATH, spark, dlt_mock)
    # Then run gold
    pipeline_runner.run_pipeline(GOLD_PATH, spark, dlt_mock)
    return dlt_mock.get_all_tables()


# ---- Pipeline Execution Tests ----

class TestGoldPipelineExecution:

    DIMENSIONS = [
        "dim_date", "dim_vendor", "dim_project", "dim_material",
        "dim_employee", "dim_geography", "dim_contract",
        "dim_cost_center", "dim_sector",
    ]
    FACTS = [
        "fact_purchase_orders", "fact_invoices", "fact_goods_receipts",
        "fact_project_costs", "fact_project_actuals",
        "fact_vendor_performance", "fact_inventory",
        "fact_contracts", "fact_sales",
    ]

    def test_gold_creates_9_dimensions(self, gold_tables):
        for d in self.DIMENSIONS:
            assert d in gold_tables, f"Missing dimension: {d}"

    def test_gold_creates_9_facts(self, gold_tables):
        for f in self.FACTS:
            assert f in gold_tables, f"Missing fact: {f}"

    def test_all_18_gold_tables_not_empty(self, gold_tables):
        for t in self.DIMENSIONS + self.FACTS:
            df = gold_tables[t]
            assert df.count() > 0, f"{t} is empty"


# ---- Dimension Tests ----

class TestDimDate:

    def test_date_range_2020_to_2030(self, gold_tables):
        df = gold_tables["dim_date"]
        years = [r["year"] for r in df.select("year").distinct().collect()]
        assert 2020 in years
        assert 2030 in years
        assert len(years) == 11

    def test_date_key_format(self, gold_tables):
        df = gold_tables["dim_date"]
        row = df.where("year = 2024 AND month = 1 AND day = 1").collect()[0]
        assert row["date_key"] == 20240101

    def test_date_columns_present(self, gold_tables):
        df = gold_tables["dim_date"]
        expected = [
            "date_key", "date", "year", "quarter", "month", "day",
            "day_of_week", "day_name", "month_name", "year_month",
            "fiscal_period", "fiscal_year", "fiscal_quarter",
        ]
        for col in expected:
            assert col in df.columns, f"Missing column: {col}"

    def test_total_rows_approximately_4018(self, gold_tables):
        df = gold_tables["dim_date"]
        # 11 years * ~365.25 days ≈ 4018
        assert 4015 <= df.count() <= 4020


class TestDimVendor:

    def test_tier_derivation(self, gold_tables):
        df = gold_tables["dim_vendor"]
        row = df.where("vendor_id = 'V001'").collect()[0]
        # V001 has performance_rating=4.5 → STRATEGIC
        assert row["tier"] == "STRATEGIC"

    def test_status_derivation(self, gold_tables):
        df = gold_tables["dim_vendor"]
        row = df.where("vendor_id = 'V001'").collect()[0]
        # V001 has prequalification_status=APPROVED → ACTIVE
        assert row["status"] == "ACTIVE"

    def test_gold_timestamp(self, gold_tables):
        df = gold_tables["dim_vendor"]
        assert "_gold_timestamp" in df.columns


class TestDimProject:

    def test_region_derivation(self, gold_tables):
        df = gold_tables["dim_project"]
        row = df.where("project_id = 'P001'").collect()[0]
        assert row["region"] == "NORTH AMERICA"

    def test_project_duration_days(self, gold_tables):
        df = gold_tables["dim_project"]
        row = df.where("project_id = 'P001'").collect()[0]
        assert row["project_duration_days"] is not None


class TestDimMaterial:

    def test_material_columns(self, gold_tables):
        df = gold_tables["dim_material"]
        assert "material_id" in df.columns
        assert "material_category" in df.columns
        assert "_is_current" in df.columns

class TestDimEmployee:

    def test_employee_columns(self, gold_tables):
        df = gold_tables["dim_employee"]
        assert "full_name" in df.columns
        assert "cost_center" in df.columns

class TestDimGeography:

    def test_geography_key_is_sha2(self, gold_tables):
        df = gold_tables["dim_geography"]
        assert "geography_key" in df.columns
        row = df.collect()[0]
        assert len(row["geography_key"]) == 64  # SHA-256 hex

    def test_region_derivation(self, gold_tables):
        df = gold_tables["dim_geography"]
        us_row = df.where("country = 'US'").collect()
        if us_row:
            assert us_row[0]["region"] == "NORTH AMERICA"

class TestDimContract:

    def test_cost_growth_pct(self, gold_tables):
        df = gold_tables["dim_contract"]
        row = df.where("contract_id = 'C001'").collect()[0]
        # change_order_value=50000, contract_value=1000000 → 5.0%
        assert abs(row["cost_growth_pct"] - 5.0) < 0.1

class TestDimCostCenter:

    def test_cost_center_distinct(self, gold_tables):
        df = gold_tables["dim_cost_center"]
        assert df.count() >= 1

class TestDimSector:

    def test_sector_union(self, gold_tables):
        df = gold_tables["dim_sector"]
        sectors = [r["sector_name"] for r in df.collect()]
        assert len(sectors) > 0


# ---- Fact Tests ----

class TestFactPurchaseOrders:

    def test_line_total_aggregated(self, gold_tables):
        df = gold_tables["fact_purchase_orders"]
        row = df.where("po_id = 'PO001'").collect()[0]
        # LI001=120050 + LI002=42500 = 162550
        assert abs(row["line_total"] - 162550.0) < 1

    def test_total_quantity_aggregated(self, gold_tables):
        df = gold_tables["fact_purchase_orders"]
        row = df.where("po_id = 'PO001'").collect()[0]
        # LI001=100 + LI002=500 = 600
        assert abs(row["total_quantity"] - 600.0) < 1


class TestFactInvoices:

    def test_aging_bucket_current(self, gold_tables):
        df = gold_tables["fact_invoices"]
        row = df.where("invoice_id = 'INV001'").collect()[0]
        # INV001 is paid, days_overdue should be 0 → CURRENT
        assert row["aging_bucket"] == "CURRENT"

    def test_aging_bucket_overdue(self, gold_tables):
        df = gold_tables["fact_invoices"]
        row = df.where("invoice_id = 'INV002'").collect()[0]
        # INV002 is unpaid, due 2024-04-15, well past due → not CURRENT
        assert row["aging_bucket"] != "CURRENT"


class TestFactGoodsReceipts:

    def test_receipt_value_calculated(self, gold_tables):
        df = gold_tables["fact_goods_receipts"]
        assert "receipt_value" in df.columns

    def test_acceptance_rate_derived(self, gold_tables):
        df = gold_tables["fact_goods_receipts"]
        assert "acceptance_rate" in df.columns


class TestFactProjectCosts:

    def test_budget_vs_actual(self, gold_tables):
        df = gold_tables["fact_project_costs"]
        assert "budget_amount" in df.columns
        assert "actual_amount" in df.columns
        assert "variance_amount" in df.columns
        assert "utilization_pct" in df.columns


class TestFactProjectActuals:

    def test_project_actuals_columns(self, gold_tables):
        df = gold_tables["fact_project_actuals"]
        assert "transaction_id" in df.columns
        assert "wbs_element" in df.columns
        assert "variance_amount" in df.columns


class TestFactVendorPerformance:

    def test_score_columns(self, gold_tables):
        df = gold_tables["fact_vendor_performance"]
        for col in ["delivery_score", "quality_score", "overall_score"]:
            assert col in df.columns


class TestFactInventory:

    def test_inventory_columns(self, gold_tables):
        df = gold_tables["fact_inventory"]
        assert "movement_type" in df.columns
        assert "quantity" in df.columns


class TestFactContracts:

    def test_items_total_aggregated(self, gold_tables):
        df = gold_tables["fact_contracts"]
        row = df.where("contract_id = 'C001'").collect()[0]
        # CI001=500000 + CI002=350000 = 850000
        assert abs(row["items_total"] - 850000.0) < 1

    def test_item_count(self, gold_tables):
        df = gold_tables["fact_contracts"]
        row = df.where("contract_id = 'C001'").collect()[0]
        assert row["item_count"] == 2


class TestFactSales:

    def test_customer_id_is_sha2(self, gold_tables):
        df = gold_tables["fact_sales"]
        row = df.collect()[0]
        assert len(row["customer_id"]) == 64  # SHA-256 hex

    def test_product_name_joined(self, gold_tables):
        df = gold_tables["fact_sales"]
        assert "product_name" in df.columns
