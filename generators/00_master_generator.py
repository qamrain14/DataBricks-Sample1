# Databricks notebook source
"""
Master Data Generator — Construction & Oil/Gas Procurement Lakehouse
======================================================================
Runs all 15 individual generators in dependency order.
Generates 100,000 records per source table (1.5M rows total).

Dependency order (respects FK relationships):
  1.  vendors          (no FK dependencies)
  2.  projects         (no FK dependencies)
  3.  materials        (self-referencing: parent_material_id)
  4.  employees        (no FK dependencies)
  5.  purchase_orders  (→ vendors, projects)
  6.  po_line_items    (→ purchase_orders, materials)
  7.  contracts        (→ vendors, projects, employees)
  8.  contract_items   (→ contracts, materials)
  9.  invoices         (→ vendors, purchase_orders, contracts, projects, employees)
  10. goods_receipts   (→ purchase_orders, po_line_items, vendors, materials, projects)
  11. project_budgets  (→ projects, employees)
  12. project_actuals  (→ projects, vendors, purchase_orders, contracts, invoices)
  13. sales_orders     (→ projects, materials, employees)
  14. inventory        (→ materials, projects, goods_receipts)
  15. vendor_perf      (→ vendors, projects, employees)

Usage:
    python 00_master_generator.py [--output-dir ../data/raw] [--records 100000]
"""

import sys
import os
import time
import argparse
import subprocess
from pathlib import Path

# ── Ensure output directory exists ───────────────────────────────────────────
DEFAULT_OUTPUT = os.path.join(os.path.dirname(__file__), '..', 'data', 'raw')

def ensure_dependencies():
    """Install required packages if not present."""
    packages = ['faker', 'pandas', 'openpyxl', 'numpy']
    for pkg in packages:
        try:
            __import__(pkg)
        except ImportError:
            print(f"  Installing {pkg}...")
            subprocess.check_call([sys.executable, '-m', 'pip', 'install', pkg, '-q'])

def run_generator(script_name: str, output_dir: str, n_records: int) -> dict:
    """Run a single generator script and return metadata."""
    script_path = os.path.join(os.path.dirname(__file__), script_name)
    start = time.time()
    
    env = os.environ.copy()
    env['OUTPUT_DIR'] = output_dir
    env['N_RECORDS'] = str(n_records)
    
    result = subprocess.run(
        [sys.executable, script_path],
        capture_output=True, text=True, env=env
    )
    elapsed = time.time() - start
    
    if result.returncode != 0:
        print(f"  ERROR in {script_name}:")
        print(result.stderr[-500:])
        return {'script': script_name, 'status': 'FAILED', 'elapsed': elapsed}
    
    # Parse output for file info
    output_file = None
    row_count = None
    file_size = None
    for line in result.stdout.splitlines():
        if 'OUTPUT:' in line:
            output_file = line.split('OUTPUT:')[-1].strip()
        if 'ROWS:' in line:
            try:
                row_count = int(line.split('ROWS:')[-1].strip().replace(',', ''))
            except:
                pass
        if 'SIZE:' in line:
            file_size = line.split('SIZE:')[-1].strip()
    
    return {
        'script': script_name,
        'status': 'OK',
        'file': output_file,
        'rows': row_count,
        'size': file_size,
        'elapsed': round(elapsed, 1)
    }

def main():
    env_output = os.environ.get('OUTPUT_DIR', DEFAULT_OUTPUT)
    env_records = int(os.environ.get('N_RECORDS', '100000'))
    
    parser = argparse.ArgumentParser(description='Generate all procurement test data')
    parser.add_argument('--output-dir', default=env_output, 
                        help='Output directory for Excel files')
    parser.add_argument('--records', type=int, default=env_records,
                        help='Number of records per file (default: 100000)')
    args = parser.parse_args()
    
    os.makedirs(args.output_dir, exist_ok=True)
    
    print("=" * 70)
    print("  PROCUREMENT LAKEHOUSE — TEST DATA GENERATOR")
    print("  Construction & Oil/Gas | 15 source tables | ~100K rows each")
    print("=" * 70)
    print(f"\n  Output directory : {os.path.abspath(args.output_dir)}")
    print(f"  Records per file : {args.records:,}")
    print(f"  Total rows target: ~{args.records * 15:,}")
    print()
    
    ensure_dependencies()
    print("  Dependencies OK\n")
    
    # ── Generator execution order ──────────────────────────────────────
    generators = [
        ("01_gen_vendors.py",         "Vendor master"),
        ("02_gen_projects.py",        "Project master"),
        ("03_gen_materials.py",       "Material/item catalog"),
        ("04_gen_employees.py",       "Employee master"),
        ("05_gen_purchase_orders.py", "Purchase order headers"),
        ("06_gen_po_line_items.py",   "PO line items"),
        ("07_gen_contracts.py",       "Contract master"),
        ("08_gen_contract_items.py",  "Contract line items (BOQ)"),
        ("09_gen_invoices.py",        "Vendor invoices"),
        ("10_gen_goods_receipts.py",  "Goods receipt notes (GRN)"),
        ("11_gen_project_budgets.py", "Project budgets by WBS"),
        ("12_gen_project_actuals.py", "Actual cost transactions"),
        ("13_gen_sales_orders.py",    "Sales orders"),
        ("14_gen_inventory.py",       "Inventory movements"),
        ("15_gen_vendor_performance.py", "Vendor performance records"),
    ]
    
    results = []
    total_rows = 0
    
    for i, (script, label) in enumerate(generators, 1):
        print(f"  [{i:02d}/15] {label:<40}", end=' ', flush=True)
        r = run_generator(script, args.output_dir, args.records)
        results.append(r)
        
        if r['status'] == 'OK':
            rows = r.get('rows') or args.records
            total_rows += rows
            print(f"✓ {rows:>7,} rows  {r.get('size',''):<10}  {r['elapsed']}s")
        else:
            print(f"✗ FAILED ({r['elapsed']}s)")
    
    # ── Summary ─────────────────────────────────────────────────────────
    print()
    print("=" * 70)
    print(f"  GENERATION COMPLETE")
    print(f"  Total rows generated : {total_rows:,}")
    failed = [r for r in results if r['status'] == 'FAILED']
    print(f"  Files succeeded      : {len(results) - len(failed)}/15")
    if failed:
        print(f"  Files failed         : {len(failed)}")
        for r in failed:
            print(f"    - {r['script']}")
    print(f"\n  Files saved to: {os.path.abspath(args.output_dir)}")
    print("=" * 70)
    
    # List generated files
    print("\n  Generated files:")
    for f in sorted(Path(args.output_dir).glob("*.xlsx")):
        size_mb = f.stat().st_size / 1_048_576
        print(f"    {f.name:<45}  {size_mb:>6.1f} MB")
    
    if failed:
        sys.exit(1)

if __name__ == '__main__':
    main()
