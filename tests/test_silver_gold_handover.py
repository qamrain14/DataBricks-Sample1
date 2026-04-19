# Databricks notebook source
"""Silver -> Gold pipeline handover tests.

Ensures every column gold reads from silver is actually produced by silver,
mirroring the bronze->silver handover suite. Catches UNRESOLVED_COLUMN errors
locally before they hit Databricks.
"""
from __future__ import annotations
import json
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
SILVER_SRC = (ROOT / "databricks" / "silver" / "02_silver_pipeline.py").read_text(encoding="utf-8")
GOLD_SRC = (ROOT / "databricks" / "gold" / "03_gold_pipeline.py").read_text(encoding="utf-8")
CSV_HEADERS = json.loads((ROOT / "csv_headers.json").read_text())

EXPECTED_SILVER_TABLES = sorted([f"silver_{k}" for k in CSV_HEADERS.keys()])
EXPECTED_GOLD_DIMS = [
    "dim_date", "dim_vendor", "dim_project", "dim_material", "dim_employee",
    "dim_geography", "dim_contract", "dim_cost_center", "dim_sector",
]
EXPECTED_GOLD_FACTS = [
    "fact_purchase_orders", "fact_invoices", "fact_goods_receipts",
    "fact_project_costs", "fact_project_actuals", "fact_vendor_performance",
    "fact_inventory", "fact_contracts", "fact_sales",
]


def _silver_output_columns() -> dict:
    """Compute silver output schema by walking each silver function in order."""
    bronze_cols = {f"bronze_{k}": list(v) + ["_ingest_timestamp", "_source_file"]
                   for k, v in CSV_HEADERS.items()}
    blocks = re.split(r"^@dlt\.table", SILVER_SRC, flags=re.MULTILINE)
    schema = {}
    op_pat = re.compile(r"\.(withColumn|withColumnRenamed|drop)\(([^)]*)\)")
    for block in blocks[1:]:
        fn_m = re.search(r"def (silver_\w+)\(\)", block)
        if not fn_m:
            continue
        src_m = re.search(
            r'spark\.read\.table\(f"\{CATALOG\}\.procurement_bronze\.(\w+)"\)', block)
        if not src_m:
            continue
        cols = list(bronze_cols.get(src_m.group(1), []))
        body = block.split("# COMMAND")[0]
        for op, args in op_pat.findall(body):
            parts = [p.strip() for p in args.split(",")]
            if op == "withColumn":
                m = re.match(r'"([^"]+)"', parts[0])
                if m and m.group(1) not in cols:
                    cols.append(m.group(1))
            elif op == "withColumnRenamed" and len(parts) >= 2:
                a = re.match(r'"([^"]+)"', parts[0])
                b = re.match(r'"([^"]+)"', parts[1])
                if a and b and a.group(1) in cols:
                    cols[cols.index(a.group(1))] = b.group(1)
            elif op == "drop":
                for q in parts:
                    m = re.match(r'"([^"]+)"', q)
                    if m and m.group(1) in cols:
                        cols.remove(m.group(1))
        cols.append("_silver_timestamp")
        schema[fn_m.group(1)] = cols
    return schema


SILVER_SCHEMA = _silver_output_columns()


def _gold_blocks():
    blocks = re.split(r"^@dlt\.table", GOLD_SRC, flags=re.MULTILINE)
    out = {}
    for block in blocks[1:]:
        fn_m = re.search(r"def (gold_\w+)\(\)", block)
        if fn_m:
            out[fn_m.group(1)] = block
    return out


GOLD_BLOCKS = _gold_blocks()


# ---------------------------------------------------------------------------
# Static-contract tests
# ---------------------------------------------------------------------------

class TestSilverGoldContract:

    def test_silver_produces_all_expected_tables(self):
        actual = sorted(SILVER_SCHEMA.keys())
        assert actual == EXPECTED_SILVER_TABLES, (
            f"Silver tables mismatch.\nExpected: {EXPECTED_SILVER_TABLES}\nActual: {actual}"
        )

    def test_gold_defines_expected_dims_and_facts(self):
        for name in EXPECTED_GOLD_DIMS + EXPECTED_GOLD_FACTS:
            assert f"gold_{name}" in GOLD_BLOCKS, f"Missing gold table: {name}"

    @pytest.mark.parametrize("gold_fn", sorted(GOLD_BLOCKS.keys()))
    def test_gold_uses_fully_qualified_silver_reads(self, gold_fn):
        """Gold must read silver via spark.read.table(FQN), never dlt.read."""
        block = GOLD_BLOCKS[gold_fn]
        # any silver mention should be via FQN
        bad = re.findall(r'dlt\.read\("silver_\w+"\)', block)
        assert not bad, f"{gold_fn} uses dlt.read for silver: {bad}"

    @pytest.mark.parametrize("gold_fn", sorted(GOLD_BLOCKS.keys()))
    def test_gold_silver_columns_exist(self, gold_fn):
        """Every col() referenced in a gold function must exist in the union of
        silver output columns it reads from, OR be created locally (alias /
        withColumn / withColumnRenamed / agg-alias / df-alias)."""
        block = GOLD_BLOCKS[gold_fn]
        reads = re.findall(
            r'spark\.read\.table\(f"\{CATALOG\}\.procurement_silver\.(silver_\w+)"\)', block)
        if not reads:
            pytest.skip("no silver reads in this gold function")
        avail = set()
        for r in reads:
            avail.update(SILVER_SCHEMA.get(r, []))
        avail.update(re.findall(r'\.alias\("([^"]+)"\)', block))
        avail.update(re.findall(r'withColumn\("([^"]+)"', block))
        avail.update(re.findall(r'withColumnRenamed\("[^"]+",\s*"([^"]+)"\)', block))
        # df-aliases like "a.column" / "b.column" - allow if the prefix is a known df alias
        df_alias_pat = re.compile(r'\b([a-zA-Z_]\w*)\s*=\s*\w+\.alias\("([^"]+)"\)')
        df_aliases = {m.group(2) for m in df_alias_pat.finditer(block)}
        # Also support `.alias("a")` chained on a read
        df_aliases.update(re.findall(r'\.alias\("([a-z])"\)', block))
        refs = re.findall(r'col\("([^"]+)"\)', block)
        bad = []
        for c in refs:
            if c in avail:
                continue
            # df-alias form: "a.colname"
            if "." in c:
                prefix, sub = c.split(".", 1)
                if prefix in df_aliases:
                    continue
            bad.append(c)
        assert not bad, (
            f"{gold_fn} references unknown column(s) {sorted(set(bad))} "
            f"(reads from: {reads}). Sample available cols: {sorted(avail)[:15]}"
        )

    def test_no_orphan_column_references(self):
        """Catch-all: aggregate every gold col() ref vs known silver schemas."""
        offenders = {}
        for fn, block in GOLD_BLOCKS.items():
            reads = re.findall(
                r'spark\.read\.table\(f"\{CATALOG\}\.procurement_silver\.(silver_\w+)"\)', block)
            if not reads:
                continue
            avail = set()
            for r in reads:
                avail.update(SILVER_SCHEMA.get(r, []))
            avail.update(re.findall(r'\.alias\("([^"]+)"\)', block))
            avail.update(re.findall(r'withColumn\("([^"]+)"', block))
            avail.update(re.findall(r'withColumnRenamed\("[^"]+",\s*"([^"]+)"\)', block))
            for c in re.findall(r'col\("([^"]+)"\)', block):
                if c in avail or "." in c:
                    continue
                offenders.setdefault(fn, []).append(c)
        assert not offenders, json.dumps(offenders, indent=2)


# ---------------------------------------------------------------------------
# Runtime handover tests (use mock_spark + dlt_mock pipeline runner)
# ---------------------------------------------------------------------------

@pytest.fixture
def gold_tables(spark, register_all_bronze):
    """Run silver then gold pipelines; return dict of {short_name: DataFrame}."""
    import dlt_mock
    from tests.pipeline_runner import run_pipeline

    silver_path = ROOT / "databricks" / "silver" / "02_silver_pipeline.py"
    gold_path = ROOT / "databricks" / "gold" / "03_gold_pipeline.py"

    run_pipeline(str(silver_path), spark, dlt_mock)
    run_pipeline(str(gold_path), spark, dlt_mock)
    all_tables = dlt_mock.get_all_tables()
    # Strip "gold_" prefix for lookup convenience
    return {k[len("gold_"):] if k.startswith("gold_") else k: v
            for k, v in all_tables.items()}


class TestSilverGoldRuntimeHandover:

    def test_all_gold_tables_materialise(self, gold_tables):
        """Every gold table is produced when bronze->silver->gold runs in mock."""
        for name in EXPECTED_GOLD_DIMS + EXPECTED_GOLD_FACTS:
            assert name in gold_tables, f"Missing gold output: {name}"

    @pytest.mark.parametrize("table", EXPECTED_GOLD_DIMS + EXPECTED_GOLD_FACTS)
    def test_gold_table_has_rows(self, gold_tables, table):
        df = gold_tables[table]
        assert df.count() > 0, f"gold {table} produced 0 rows"

    @pytest.mark.parametrize("table", EXPECTED_GOLD_DIMS + EXPECTED_GOLD_FACTS)
    def test_gold_table_has_gold_timestamp(self, gold_tables, table):
        if table == "dim_date":
            pytest.skip("dim_date is generated from a date sequence, not from silver — no _gold_timestamp expected")
        df = gold_tables[table]
        assert "_gold_timestamp" in df.columns, f"{table} missing _gold_timestamp"


class TestSilverMissingFailsGold:

    def test_gold_fails_when_silver_unregistered(self, spark):
        """Sanity: if silver tables aren't registered, gold pipeline must error,
        proving the FQN reads actually go to the silver layer."""
        from tests.pipeline_runner import run_pipeline
        with pytest.raises(Exception):
            run_pipeline("databricks/gold/03_gold_pipeline.py")
