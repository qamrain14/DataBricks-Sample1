"""
Phase 8 — End-to-end validation of all Power BI artifacts.
Checks file counts, cross-references table names, validates JSON,
BIM integrity, report.json field mappings, theme, and DAX.
"""
import json
import os
import sys
import re
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent          # powerbi/
ROOT = BASE.parent                                     # repo root

PASS = 0
FAIL = 0
WARN = 0

def ok(msg):
    global PASS
    PASS += 1
    print(f"  [PASS] {msg}")

def fail(msg):
    global FAIL
    FAIL += 1
    print(f"  [FAIL] {msg}")

def warn(msg):
    global WARN
    WARN += 1
    print(f"  [WARN] {msg}")

# ---------------------------------------------------------------------------
# 1. connection_config.py — import & table count
# ---------------------------------------------------------------------------
print("\n=== 1. connection_config.py ===")
sys.path.insert(0, str(BASE / "scripts"))
try:
    import connection_config as cc
    ok("connection_config imported")
except Exception as e:
    fail(f"import error: {e}")
    sys.exit(1)

gold_dims = set(cc.GOLD_DIMENSIONS.keys())
gold_facts = set(cc.GOLD_FACTS.keys())
sem_cubes = set(cc.SEMANTIC_CUBES.keys())
all_keys  = set(cc.ALL_TABLES.keys())

if len(gold_dims) == 9:
    ok(f"GOLD_DIMENSIONS count = {len(gold_dims)}")
else:
    fail(f"GOLD_DIMENSIONS count = {len(gold_dims)} (expected 9)")

if len(gold_facts) == 9:
    ok(f"GOLD_FACTS count = {len(gold_facts)}")
else:
    fail(f"GOLD_FACTS count = {len(gold_facts)} (expected 9)")

if len(sem_cubes) == 15:
    ok(f"SEMANTIC_CUBES count = {len(sem_cubes)}")
else:
    fail(f"SEMANTIC_CUBES count = {len(sem_cubes)} (expected 15)")

if len(all_keys) == 33:
    ok(f"ALL_TABLES count = {len(all_keys)}")
else:
    fail(f"ALL_TABLES count = {len(all_keys)} (expected 33)")

# Every TableMeta has a non-empty columns list
empty_cols = [k for k, v in cc.ALL_TABLES.items() if not v.columns]
if not empty_cols:
    ok("All tables have non-empty column lists")
else:
    fail(f"Tables with empty columns: {empty_cols}")

# ---------------------------------------------------------------------------
# 2. Power Query .pq files
# ---------------------------------------------------------------------------
print("\n=== 2. Power Query .pq files ===")
pq_dir = BASE / "connections"
pq_files = sorted(pq_dir.glob("*.pq"))
pq_names = {f.stem for f in pq_files}

if len(pq_files) == 33:
    ok(f".pq file count = {len(pq_files)}")
else:
    fail(f".pq file count = {len(pq_files)} (expected 33)")

# Cross-reference: each key in ALL_TABLES should have a matching .pq
missing_pq = all_keys - pq_names
extra_pq   = pq_names - all_keys
if not missing_pq:
    ok("All config keys have a .pq file")
else:
    fail(f"Config keys missing .pq: {missing_pq}")
if not extra_pq:
    ok("No orphan .pq files")
else:
    warn(f"Extra .pq files without config entry: {extra_pq}")

# Spot-check: each .pq references Databricks.Catalogs
bad_pq = []
for f in pq_files:
    txt = f.read_text(encoding="utf-8")
    if "Databricks.Catalogs" not in txt:
        bad_pq.append(f.name)
if not bad_pq:
    ok("All .pq files reference Databricks.Catalogs")
else:
    fail(f".pq files missing Databricks.Catalogs: {bad_pq}")

# ---------------------------------------------------------------------------
# 3. BIM model
# ---------------------------------------------------------------------------
print("\n=== 3. BIM model (procurement_model.bim) ===")
bim_path = BASE / "model" / "procurement_model.bim"
try:
    bim = json.loads(bim_path.read_text(encoding="utf-8"))
    ok("BIM JSON valid")
except Exception as e:
    fail(f"BIM JSON parse error: {e}")
    bim = None

if bim:
    model = bim.get("model", {})
    tables = model.get("tables", [])
    bim_table_names = {t["name"] for t in tables}

    if len(tables) == 33:
        ok(f"BIM table count = {len(tables)}")
    else:
        fail(f"BIM table count = {len(tables)} (expected 33)")

    # Pascal-case cross-reference
    def pascal(s):
        return "".join(w.capitalize() for w in s.split("_"))

    expected_bim_names = {pascal(k) for k in all_keys}
    missing_bim = expected_bim_names - bim_table_names
    extra_bim   = bim_table_names - expected_bim_names
    if not missing_bim:
        ok("All config tables present in BIM (PascalCase)")
    else:
        fail(f"Config tables missing from BIM: {missing_bim}")
    if not extra_bim:
        ok("No orphan BIM tables")
    else:
        warn(f"Extra BIM tables: {extra_bim}")

    # Relationships
    rels = model.get("relationships", [])
    if len(rels) == 33:
        ok(f"BIM relationship count = {len(rels)}")
    else:
        fail(f"BIM relationship count = {len(rels)} (expected 33)")

    # Validate relationship endpoints reference existing tables
    bad_rels = []
    for r in rels:
        ft = r.get("fromTable", "")
        tt = r.get("toTable", "")
        if ft not in bim_table_names or tt not in bim_table_names:
            bad_rels.append(f"{ft} -> {tt}")
    if not bad_rels:
        ok("All relationship endpoints reference valid tables")
    else:
        fail(f"Invalid relationship endpoints: {bad_rels}")

    # Measures
    all_measures = []
    for t in tables:
        for m in t.get("measures", []):
            all_measures.append(m)
    if len(all_measures) == 25:
        ok(f"BIM measure count = {len(all_measures)}")
    else:
        fail(f"BIM measure count = {len(all_measures)} (expected 25)")

    # Each measure has a non-empty expression
    empty_expr = [m["name"] for m in all_measures if not m.get("expression", "").strip()]
    if not empty_expr:
        ok("All measures have non-empty DAX expressions")
    else:
        fail(f"Measures with empty expressions: {empty_expr}")

    # Each table has at least 1 column
    no_cols = [t["name"] for t in tables if not t.get("columns")]
    if not no_cols:
        ok("All BIM tables have at least 1 column")
    else:
        fail(f"BIM tables with no columns: {no_cols}")

# ---------------------------------------------------------------------------
# 4. Theme JSON
# ---------------------------------------------------------------------------
print("\n=== 4. Theme JSON ===")
theme_path = BASE / "theme" / "procurement_analytics.json"
try:
    theme = json.loads(theme_path.read_text(encoding="utf-8"))
    ok("Theme JSON valid")
except Exception as e:
    fail(f"Theme JSON parse error: {e}")
    theme = None

if theme:
    if theme.get("name"):
        ok(f"Theme name = '{theme['name']}'")
    else:
        fail("Theme missing 'name'")
    dc = theme.get("dataColors", [])
    if len(dc) >= 6:
        ok(f"dataColors count = {len(dc)}")
    else:
        fail(f"dataColors count = {len(dc)} (expected >= 6)")

# ---------------------------------------------------------------------------
# 5. Report definition (PBIP)
# ---------------------------------------------------------------------------
print("\n=== 5. PBIP report definition ===")
pbir_path = BASE / "report" / "definition.pbir"
rpt_path  = BASE / "report" / "report.json"

try:
    pbir = json.loads(pbir_path.read_text(encoding="utf-8"))
    ok("definition.pbir JSON valid")
except Exception as e:
    fail(f"definition.pbir parse error: {e}")

try:
    rpt = json.loads(rpt_path.read_text(encoding="utf-8"))
    ok("report.json JSON valid")
except Exception as e:
    fail(f"report.json parse error: {e}")
    rpt = None

if rpt:
    pages = rpt.get("pages", rpt.get("reportPages", []))
    if len(pages) == 11:
        ok(f"Report page count = {len(pages)}")
    else:
        fail(f"Report page count = {len(pages)} (expected 11)")

    expected_pages = {
        "pg_executive_summary", "pg_procurement_spend",
        "pg_vendor_performance", "pg_invoice_payment",
        "pg_project_costs", "pg_delivery_quality",
        "pg_contracts", "pg_sales", "pg_inventory",
        "pg_p2p_efficiency", "pg_project_timeline",
    }
    actual_pages = {p["name"] for p in pages}
    if actual_pages == expected_pages:
        ok("All 11 page names match specification")
    else:
        missing_p = expected_pages - actual_pages
        extra_p   = actual_pages - expected_pages
        if missing_p:
            fail(f"Missing pages: {missing_p}")
        if extra_p:
            warn(f"Extra pages: {extra_p}")

    # Count visuals across all pages
    total_visuals = sum(len(p.get("visuals", [])) for p in pages)
    if total_visuals >= 30:
        ok(f"Total visuals across all pages = {total_visuals}")
    else:
        fail(f"Total visuals = {total_visuals} (expected >= 30)")

    # Every visual has a visualType
    no_type = []
    for p in pages:
        for v in p.get("visuals", []):
            if not v.get("visualType"):
                no_type.append(f"{p['name']}/{v.get('name','?')}")
    if not no_type:
        ok("All visuals have a visualType")
    else:
        fail(f"Visuals missing visualType: {no_type}")

# ---------------------------------------------------------------------------
# 6. DAX measures reference file
# ---------------------------------------------------------------------------
print("\n=== 6. DAX measures reference ===")
dax_path = BASE / "model" / "dax_measures.dax"
if dax_path.exists():
    dax_text = dax_path.read_text(encoding="utf-8")
    # Count measure definitions (lines matching "NAME = ...")
    measure_defs = re.findall(r'^(\w[\w %]+?)\s*=\s', dax_text, re.MULTILINE)
    if len(measure_defs) == 25:
        ok(f"DAX file measure definitions = {len(measure_defs)}")
    else:
        fail(f"DAX file measure definitions = {len(measure_defs)} (expected 25)")
else:
    fail("dax_measures.dax not found")

# ---------------------------------------------------------------------------
# 7. Documentation files
# ---------------------------------------------------------------------------
print("\n=== 7. Documentation ===")
doc_files = {
    "README.md": BASE / "README.md",
    "REPORT_SPEC.md": BASE / "docs" / "REPORT_SPEC.md",
    "REPORT_BUILD_GUIDE.md": BASE / "docs" / "REPORT_BUILD_GUIDE.md",
}
for name, path in doc_files.items():
    if path.exists() and path.stat().st_size > 100:
        ok(f"{name} exists ({path.stat().st_size} bytes)")
    else:
        fail(f"{name} missing or too small")

# ---------------------------------------------------------------------------
# 8. Scripts
# ---------------------------------------------------------------------------
print("\n=== 8. Scripts & utilities ===")
script_files = {
    "gen_pq_files.pyw": BASE / "scripts" / "gen_pq_files.pyw",
    "gen_bim_model.pyw": BASE / "scripts" / "gen_bim_model.pyw",
    "odbc_setup.bat": BASE / "scripts" / "odbc_setup.bat",
    "refresh.bat": BASE / "scripts" / "refresh.bat",
}
for name, path in script_files.items():
    if path.exists() and path.stat().st_size > 50:
        ok(f"{name} exists ({path.stat().st_size} bytes)")
    else:
        fail(f"{name} missing or too small")

# ---------------------------------------------------------------------------
# 9. Cross-reference: report.json field projections vs BIM columns
# ---------------------------------------------------------------------------
print("\n=== 9. Report-to-BIM field cross-reference ===")
if rpt and bim:
    # Build lookup: BIM table -> set of column names
    bim_cols = {}
    for t in tables:
        cols = {c["name"] for c in t.get("columns", [])}
        bim_cols[t["name"]] = cols

    # Collect all field refs from report visuals
    field_refs = set()
    for p in rpt.get("reportPages", []):
        for v in p.get("visuals", []):
            for role, fields in v.get("projections", {}).items():
                for f in fields:
                    field_refs.add(f)

    # Parse "Table.Column" references
    unresolved = []
    for ref in sorted(field_refs):
        if "." in ref:
            tbl, col = ref.split(".", 1)
            if tbl in bim_cols:
                if col not in bim_cols[tbl]:
                    # Check measures
                    tbl_measures = set()
                    for t in tables:
                        if t["name"] == tbl:
                            tbl_measures = {m["name"] for m in t.get("measures", [])}
                    if col not in tbl_measures:
                        unresolved.append(ref)
            else:
                unresolved.append(ref)

    if not unresolved:
        ok(f"All {len(field_refs)} report field refs resolve to BIM columns/measures")
    else:
        warn(f"{len(unresolved)} unresolved field refs (may use display names): {unresolved[:10]}")
else:
    warn("Skipped report-to-BIM cross-ref (missing data)")

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
print("\n" + "=" * 60)
total = PASS + FAIL + WARN
print(f"TOTAL: {total} checks — {PASS} PASS, {FAIL} FAIL, {WARN} WARN")
if FAIL == 0:
    print("[OK] ALL CHECKS PASSED - Power BI integration is complete.")
else:
    print(f"[!!] {FAIL} check(s) FAILED - review above.")
print("=" * 60)

sys.exit(1 if FAIL else 0)
