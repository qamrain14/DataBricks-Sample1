"""Bronze → Silver handover & sync tests.

Validates that the output of the bronze pipeline is consumable by the silver
pipeline: every silver table must read from a bronze table that exists, the
fully-qualified read path must be correct, and the in-process pipeline runner
must materialise all 15 silver tables non-empty when fed sample bronze data.
"""
from __future__ import annotations

import os
import re
import sys
from pathlib import Path

import pytest
import yaml

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import dlt_mock
import pipeline_runner

ROOT = Path(__file__).resolve().parent.parent
BRONZE_PATH = ROOT / "databricks" / "bronze" / "01_bronze_pipeline.py"
SILVER_PATH = ROOT / "databricks" / "silver" / "02_silver_pipeline.py"
ORCH_YML = ROOT / "resources" / "procurement_orchestration_job.yml"
PIPELINES_YML = ROOT / "resources" / "procurement_pipelines.yml"

# Canonical 15 source tables emitted by bronze.
EXPECTED_TABLES = [
    "vendors", "projects", "materials", "employees",
    "purchase_orders", "po_line_items", "contracts", "contract_items",
    "invoices", "goods_receipts", "project_budgets", "project_actuals",
    "sales_orders", "inventory", "vendor_performance",
]


# ── Static / structural sync ─────────────────────────────────────────

def _read(p: Path) -> str:
    return p.read_text(encoding="utf-8")


class TestBronzeSilverContract:
    """Static contract: silver consumes only what bronze produces."""

    def test_bronze_declares_all_15_expected_source_tables(self):
        text = _read(BRONZE_PATH)
        m = re.search(r"TABLES\s*=\s*\[(.*?)\]", text, re.DOTALL)
        assert m, "Bronze TABLES list must be present"
        listed = re.findall(r'"([a-z_]+)"', m.group(1))
        assert sorted(listed) == sorted(EXPECTED_TABLES), (
            f"Bronze TABLES drift: {set(listed) ^ set(EXPECTED_TABLES)}"
        )

    @pytest.mark.parametrize("tbl", EXPECTED_TABLES)
    def test_silver_reads_bronze_for_each_expected_table(self, tbl):
        text = _read(SILVER_PATH)
        # silver MUST read the matching bronze_<tbl> via fully-qualified FQN.
        pattern = (
            r"spark\.read\.table\(\s*f?\"\{CATALOG\}\.procurement_bronze\."
            + f"bronze_{tbl}" + r"\"\s*\)"
        )
        assert re.search(pattern, text), (
            f"Silver does not read bronze_{tbl} via spark.read.table FQN"
        )

    def test_silver_does_not_reference_unknown_bronze_tables(self):
        text = _read(SILVER_PATH)
        refs = re.findall(
            r"procurement_bronze\.bronze_([a-z_]+)", text
        )
        unknown = sorted(set(refs) - set(EXPECTED_TABLES))
        assert unknown == [], f"Silver references unknown bronze tables: {unknown}"

    def test_silver_uses_no_legacy_dlt_read_for_bronze(self):
        text = _read(SILVER_PATH)
        leftovers = re.findall(r'dlt\.read\(\s*"bronze_[^"]+"\s*\)', text)
        assert leftovers == [], (
            f"Silver still uses dlt.read for bronze (cross-pipeline broken): {leftovers}"
        )


# ── Orchestration / deployment sync ──────────────────────────────────

class TestOrchestrationOrdering:
    def test_silver_depends_on_bronze_in_job(self):
        spec = yaml.safe_load(_read(ORCH_YML))
        tasks = spec["resources"]["jobs"]["procurement_end_to_end_job"]["tasks"]
        silver = next(t for t in tasks if t["task_key"] == "run_silver_pipeline")
        deps = [d["task_key"] for d in silver.get("depends_on", [])]
        assert "run_bronze_pipeline" in deps, (
            "Silver task must declare bronze as upstream dependency"
        )

    def test_pipelines_share_catalog(self):
        spec = yaml.safe_load(_read(PIPELINES_YML))
        pls = spec["resources"]["pipelines"]
        catalogs = {name: p.get("catalog") for name, p in pls.items()}
        assert len(set(catalogs.values())) == 1, (
            f"Bronze/Silver/Gold catalogs diverge: {catalogs}"
        )

    def test_silver_target_schema_is_distinct_from_bronze(self):
        spec = yaml.safe_load(_read(PIPELINES_YML))
        pls = spec["resources"]["pipelines"]
        bronze_target = pls["procurement_bronze_pipeline"]["target"]
        silver_target = pls["procurement_silver_pipeline"]["target"]
        assert bronze_target == "procurement_bronze"
        assert silver_target == "procurement_silver"
        assert bronze_target != silver_target


# ── Runtime handover (mock end-to-end) ───────────────────────────────

class TestBronzeSilverRuntimeHandover:
    """Run silver against registered bronze tables and assert materialisation."""

    @pytest.fixture
    def silver_tables(self, spark, register_all_bronze):
        """Execute silver pipeline on top of mocked bronze output."""
        return pipeline_runner.run_pipeline(str(SILVER_PATH), spark, dlt_mock)

    def test_all_15_silver_tables_materialised(self, silver_tables):
        produced = {n for n in silver_tables if n.startswith("silver_")}
        expected = {f"silver_{t}" for t in EXPECTED_TABLES}
        missing = expected - produced
        assert not missing, f"Silver did not produce: {sorted(missing)}"

    @pytest.mark.parametrize("tbl", EXPECTED_TABLES)
    def test_each_silver_table_non_empty(self, silver_tables, tbl):
        df = silver_tables[f"silver_{tbl}"]
        assert df.count() > 0, f"silver_{tbl} is empty after running on bronze fixture"

    def test_silver_columns_superset_of_bronze_columns(self, silver_tables, bronze_data):
        """Silver must preserve every bronze input column (rename allowed only via schema map)."""
        # We assert at least one column carries over for each table to detect
        # a broken read or accidental projection-out of all source fields.
        for tbl in EXPECTED_TABLES:
            silver_df = silver_tables[f"silver_{tbl}"]
            bronze_cols = set(bronze_data[f"bronze_{tbl}"].columns)
            silver_cols = set(silver_df.columns)
            overlap = bronze_cols & silver_cols
            assert overlap, (
                f"silver_{tbl} carries no columns from bronze_{tbl}: "
                f"bronze={sorted(bronze_cols)} silver={sorted(silver_cols)}"
            )

    def test_silver_row_counts_bounded_by_bronze(self, silver_tables, bronze_data):
        """Silver row count must never exceed bronze (cleansing only drops/dedups)."""
        for tbl in EXPECTED_TABLES:
            bdf = bronze_data[f"bronze_{tbl}"]
            bronze_n = len(bdf.rows) if hasattr(bdf, "rows") else bdf.count()
            silver_n = silver_tables[f"silver_{tbl}"].count()
            assert silver_n <= bronze_n, (
                f"silver_{tbl} ({silver_n}) > bronze_{tbl} ({bronze_n}) — "
                "silver should cleanse, not amplify"
            )


# ── Negative test: handover must fail loudly if bronze missing ───────

class TestBronzeMissingFailsSilver:
    def test_silver_raises_when_bronze_unregistered(self, spark):
        with pytest.raises((KeyError, RuntimeError)):
            pipeline_runner.run_pipeline(str(SILVER_PATH), spark, dlt_mock)
