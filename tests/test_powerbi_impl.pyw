"""Power BI integration tests — kept as .pyw to avoid Databricks extension overwrite."""
import json
import os
import sys
import pathlib
import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "powerbi" / "scripts"))

from connection_config import (
    DatabricksConnection,
    TableMeta,
    GOLD_DIMENSIONS,
    GOLD_FACTS,
    SEMANTIC_CUBES,
    ALL_TABLES,
    CONNECTION,
    get_power_query_source,
)

PQ_DIR = ROOT / "powerbi" / "connections"
BIM_PATH = ROOT / "powerbi" / "model" / "procurement_model.bim"


# ---------------------------------------------------------------------------
# DatabricksConnection
# ---------------------------------------------------------------------------
class TestDatabricksConnection:
    def test_defaults(self):
        c = CONNECTION
        assert c.host == "dbc-760a206e-c226.cloud.databricks.com"
        assert c.catalog == "workspace"
        assert c.port == 443

    def test_jdbc_url(self):
        url = CONNECTION.jdbc_url
        assert url.startswith("jdbc:spark://")
        assert CONNECTION.host in url

    def test_odbc_connection_string(self):
        cs = CONNECTION.odbc_connection_string
        assert "DRIVER=" in cs.upper() or "DSN=" in cs.upper()

    def test_frozen(self):
        with pytest.raises(AttributeError):
            CONNECTION.host = "other"


# ---------------------------------------------------------------------------
# TableMeta
# ---------------------------------------------------------------------------
class TestTableMeta:
    def test_full_name(self):
        t = list(GOLD_DIMENSIONS.values())[0]
        assert t.full_name.startswith("workspace.")

    def test_columns_non_empty(self):
        for key, meta in ALL_TABLES.items():
            assert len(meta.columns) > 0, f"{key} has no columns"


# ---------------------------------------------------------------------------
# Table registry
# ---------------------------------------------------------------------------
class TestTableRegistry:
    def test_total_count(self):
        assert len(ALL_TABLES) == 33

    def test_dims_count(self):
        assert len(GOLD_DIMENSIONS) == 9

    def test_facts_count(self):
        assert len(GOLD_FACTS) == 9

    def test_cubes_count(self):
        assert len(SEMANTIC_CUBES) == 15

    def test_union_equals_all(self):
        union = {**GOLD_DIMENSIONS, **GOLD_FACTS, **SEMANTIC_CUBES}
        assert set(union.keys()) == set(ALL_TABLES.keys())

    def test_dim_date_columns(self):
        cols = GOLD_DIMENSIONS["dim_date"].columns
        for c in ("date_key", "year", "quarter", "month", "fiscal_year"):
            assert c in cols, f"dim_date missing {c}"

    def test_dim_vendor_columns(self):
        cols = GOLD_DIMENSIONS["dim_vendor"].columns
        for c in ("vendor_id", "vendor_name", "vendor_type"):
            assert c in cols

    def test_fact_purchase_orders_columns(self):
        cols = GOLD_FACTS["fact_purchase_orders"].columns
        for c in ("po_id", "vendor_id", "project_id", "po_value"):
            assert c in cols

    def test_cube_procurement_spend_columns(self):
        cols = SEMANTIC_CUBES["cube_procurement_spend"].columns
        for c in ("vendor_id", "total_spend", "yoy_growth_pct"):
            assert c in cols

    def test_schemas(self):
        for key, meta in GOLD_DIMENSIONS.items():
            assert meta.schema == "procurement_gold", f"{key} schema"
        for key, meta in GOLD_FACTS.items():
            assert meta.schema == "procurement_gold", f"{key} schema"
        for key, meta in SEMANTIC_CUBES.items():
            if key == "cube_budget_vs_actual":
                assert meta.schema == "procurement_gold"
            else:
                assert meta.schema == "procurement_semantic", f"{key} schema"


# ---------------------------------------------------------------------------
# Power Query .pq files
# ---------------------------------------------------------------------------
class TestPowerQueryFiles:
    def test_pq_dir_exists(self):
        assert PQ_DIR.is_dir()

    def test_pq_count(self):
        pq_files = list(PQ_DIR.glob("*.pq"))
        assert len(pq_files) == 33

    def test_each_table_has_pq(self):
        for key in ALL_TABLES:
            assert (PQ_DIR / f"{key}.pq").exists(), f"Missing {key}.pq"

    def test_pq_contains_databricks_source(self):
        for key in ALL_TABLES:
            text = (PQ_DIR / f"{key}.pq").read_text()
            assert "Databricks.Catalogs(" in text, f"{key}.pq missing source"

    def test_pq_renames_to_pascal(self):
        text = (PQ_DIR / "dim_vendor.pq").read_text()
        assert "VendorId" in text or "VendorName" in text

    def test_pq_removes_internal_cols(self):
        text = (PQ_DIR / "dim_vendor.pq").read_text()
        assert "_is_current" not in text.split("RemoveInternal")[-1] or \
               "RemoveInternal" in text

    def test_get_power_query_source(self):
        meta = ALL_TABLES["dim_vendor"]
        src = get_power_query_source(meta)
        assert "Databricks.Catalogs(" in src


# ---------------------------------------------------------------------------
# BIM model
# ---------------------------------------------------------------------------
class TestBimModel:
    @pytest.fixture(scope="class")
    def bim(self):
        return json.loads(BIM_PATH.read_text(encoding="utf-8-sig"))

    def test_bim_exists(self):
        assert BIM_PATH.exists()

    def test_compatibility(self, bim):
        assert bim["compatibilityLevel"] == 1600

    def test_table_count(self, bim):
        tables = bim["model"]["tables"]
        assert len(tables) == 33

    def test_table_names(self, bim):
        names = {t["name"] for t in bim["model"]["tables"]}
        # spot-check a few
        assert "DimVendor" in names
        assert "FactPurchaseOrders" in names
        assert "CubeProcurementSpend" in names

    def test_relationship_count(self, bim):
        rels = bim["model"]["relationships"]
        assert len(rels) == 33

    def test_measures_exist(self, bim):
        measures = []
        for t in bim["model"]["tables"]:
            measures.extend(t.get("measures", []))
        assert len(measures) == 28

    def test_measure_has_dax(self, bim):
        for t in bim["model"]["tables"]:
            for m in t.get("measures", []):
                assert "expression" in m, f"Measure {m['name']} missing DAX"
