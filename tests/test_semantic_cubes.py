"""
Tests for the 15 semantic cube SQL files in databricks/cubes/.
Validates catalog/schema references, view types, date columns, and SQL structure.
"""
import glob
import os
import re

import pytest

CUBES_DIR = os.path.join(
    os.path.dirname(__file__), os.pardir, "databricks", "cubes"
)
CUBE_FILES = sorted(glob.glob(os.path.join(CUBES_DIR, "*.sql")))

EXPECTED_CUBES = [
    "01_cube_procurement_spend.sql",
    "02_cube_vendor_performance.sql",
    "03_cube_contract_status.sql",
    "04_cube_invoice_aging.sql",
    "05_cube_project_costs.sql",
    "06_cube_goods_receipt_quality.sql",
    "07_cube_budget_vs_actual.sql",
    "08_cube_sales_analysis.sql",
    "09_cube_inventory_status.sql",
    "10_cube_spend_by_sector.sql",
    "11_cube_delivery_performance.sql",
    "12_cube_cost_variance.sql",
    "13_cube_vendor_risk.sql",
    "14_cube_project_timeline.sql",
    "15_cube_procurement_efficiency.sql",
]


def _read(path: str) -> str:
    with open(path, encoding="utf-8") as f:
        return f.read()


class TestCubeInventory:
    def test_all_expected_files_exist(self):
        actual = sorted(os.path.basename(p) for p in CUBE_FILES)
        assert actual == EXPECTED_CUBES

    def test_no_unexpected_files(self):
        actual = set(os.path.basename(p) for p in CUBE_FILES)
        expected = set(EXPECTED_CUBES)
        assert actual - expected == set()


class TestCatalogSchema:
    @pytest.mark.parametrize(
        "cube_path", CUBE_FILES, ids=lambda p: os.path.basename(p)
    )
    def test_no_procurement_dev_catalog(self, cube_path):
        sql = _read(cube_path)
        assert "procurement_dev" not in sql.lower()

    @pytest.mark.parametrize(
        "cube_path", CUBE_FILES, ids=lambda p: os.path.basename(p)
    )
    def test_create_view_uses_workspace_semantic(self, cube_path):
        sql = _read(cube_path)
        match = re.search(r"CREATE\s+OR\s+REPLACE\s+VIEW\s+(\S+)", sql, re.IGNORECASE)
        assert match
        assert match.group(1).startswith("workspace.procurement_semantic.")

    @pytest.mark.parametrize(
        "cube_path", CUBE_FILES, ids=lambda p: os.path.basename(p)
    )
    def test_gold_refs_use_workspace_gold(self, cube_path):
        sql = _read(cube_path)
        gold_refs = re.findall(r"\bworkspace\.procurement_gold\.\w+", sql)
        other_gold = re.findall(r"\bprocurement_gold\.\w+", sql)
        assert len(gold_refs) == len(other_gold)


class TestViewType:
    @pytest.mark.parametrize(
        "cube_path", CUBE_FILES, ids=lambda p: os.path.basename(p)
    )
    def test_no_materialized_view(self, cube_path):
        sql = _read(cube_path)
        assert "MATERIALIZED VIEW" not in sql.upper()

    @pytest.mark.parametrize(
        "cube_path", CUBE_FILES, ids=lambda p: os.path.basename(p)
    )
    def test_has_create_or_replace_view(self, cube_path):
        sql = _read(cube_path)
        assert re.search(r"CREATE\s+OR\s+REPLACE\s+VIEW", sql, re.IGNORECASE)


class TestDateColumn:
    @pytest.mark.parametrize(
        "cube_path", CUBE_FILES, ids=lambda p: os.path.basename(p)
    )
    def test_no_full_date_reference(self, cube_path):
        sql = _read(cube_path)
        assert "dd.full_date" not in sql.lower()


class TestCommentHeaders:
    @pytest.mark.parametrize(
        "cube_path", CUBE_FILES, ids=lambda p: os.path.basename(p)
    )
    def test_header_mentions_semantic_schema(self, cube_path):
        sql = _read(cube_path)
        assert "workspace.procurement_semantic" in sql[:500]

    @pytest.mark.parametrize(
        "cube_path", CUBE_FILES, ids=lambda p: os.path.basename(p)
    )
    def test_has_comment_clause(self, cube_path):
        sql = _read(cube_path)
        assert re.search(r"COMMENT\s+'[^']+'", sql, re.IGNORECASE)


class TestSQLStructure:
    @pytest.mark.parametrize(
        "cube_path", CUBE_FILES, ids=lambda p: os.path.basename(p)
    )
    def test_single_create_view_per_file(self, cube_path):
        sql = _read(cube_path)
        matches = re.findall(r"CREATE\s+OR\s+REPLACE\s+VIEW", sql, re.IGNORECASE)
        assert len(matches) == 1

    @pytest.mark.parametrize(
        "cube_path", CUBE_FILES, ids=lambda p: os.path.basename(p)
    )
    def test_view_name_matches_filename(self, cube_path):
        basename = os.path.basename(cube_path).replace(".sql", "")
        expected_suffix = re.sub(r"^\d+_", "", basename)
        sql = _read(cube_path)
        match = re.search(
            r"CREATE\s+OR\s+REPLACE\s+VIEW\s+\S+\.(\w+)", sql, re.IGNORECASE
        )
        assert match
        assert match.group(1) == expected_suffix
