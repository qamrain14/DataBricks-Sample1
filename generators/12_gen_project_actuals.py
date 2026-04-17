# Databricks notebook source
"""
Generator 12: Project Actuals
================================
Output : data/raw/project_actuals.xlsx
Records: 100,000
Seed   : 42
Domain : Financial — Cost Tracking

Business Rules:
  - FK → projects, vendors, purchase_orders, contracts, invoices
  - transaction_id = TXN-{YEAR}-{8digit}
  - variance = actual_amount - budget_amount
  - cost_status: POSTED / ACCRUED / COMMITTED / FORECAST
  - change_order_flag boolean
"""

import os
import random
import numpy as np
import pandas as pd
from faker import Faker
from datetime import datetime, timedelta

# ── Configuration ────────────────────────────────────────────────────────────
N         = int(os.getenv('N_RECORDS', 100_000))
OUT_DIR   = os.getenv('OUTPUT_DIR', os.path.join(os.path.dirname(__file__), '..', 'data', 'raw'))
OUT_FILE  = os.path.join(OUT_DIR, 'project_actuals.xlsx')
SEED      = 42

fake = Faker()
Faker.seed(SEED)
np.random.seed(SEED)
random.seed(SEED)

os.makedirs(OUT_DIR, exist_ok=True)

# ── Load FK references ────────────────────────────────────────────────────────
projects_df  = pd.read_excel(os.path.join(OUT_DIR, 'projects.xlsx'), sheet_name='projects')
vendors_df   = pd.read_excel(os.path.join(OUT_DIR, 'vendors.xlsx'), sheet_name='vendors')
po_df        = pd.read_excel(os.path.join(OUT_DIR, 'purchase_orders.xlsx'), sheet_name='purchase_orders')
contracts_df = pd.read_excel(os.path.join(OUT_DIR, 'contracts.xlsx'), sheet_name='contracts')
invoices_df  = pd.read_excel(os.path.join(OUT_DIR, 'invoices.xlsx'), sheet_name='invoices')

project_ids  = projects_df['project_id'].tolist()
vendor_ids   = vendors_df['vendor_id'].tolist()
po_ids       = po_df['po_id'].tolist()
contract_ids = contracts_df['contract_id'].tolist()
invoice_ids  = invoices_df['invoice_id'].tolist()

# ── Lookup tables ─────────────────────────────────────────────────────────────
COST_STATUS   = ['POSTED', 'ACCRUED', 'COMMITTED', 'FORECAST']
COST_STATUS_W = [0.40, 0.25, 0.20, 0.15]

COST_TYPES    = ['LABOUR', 'MATERIAL', 'EQUIPMENT', 'SUBCONTRACT', 'OVERHEAD', 'FREIGHT', 'INSURANCE']
COST_TYPE_W   = [0.20, 0.30, 0.10, 0.20, 0.08, 0.07, 0.05]

WBS_L1 = ['01', '02', '03', '04', '05', '06', '07', '08']
WBS_L2 = ['01', '02', '03', '04', '05']


# ── Generation ────────────────────────────────────────────────────────────────
def generate_project_actuals(n):
    rows = []

    budget_values = np.random.lognormal(mean=10.5, sigma=1.5, size=n)
    budget_values = np.clip(budget_values, 1_000, 10_000_000)

    for i in range(n):
        year = random.randint(2019, 2025)
        txn_id = f"TXN-{year}-{i+1:08d}"

        project_id = random.choice(project_ids)
        vendor_id  = random.choice(vendor_ids)
        cost_status = random.choices(COST_STATUS, COST_STATUS_W)[0]
        cost_type   = random.choices(COST_TYPES, COST_TYPE_W)[0]

        wbs_code = f"{random.choice(WBS_L1)}.{random.choice(WBS_L2)}"

        # Link to PO or contract
        if random.random() < 0.5:
            po_id = random.choice(po_ids)
            contract_id = None
        else:
            po_id = None
            contract_id = random.choice(contract_ids)

        invoice_id = random.choice(invoice_ids) if cost_status == 'POSTED' else None

        budget_amount = round(float(budget_values[i]), 2)
        # Actual varies from budget by -20% to +30%
        variance_factor = random.uniform(-0.20, 0.30)
        actual_amount = round(budget_amount * (1 + variance_factor), 2)
        variance = round(actual_amount - budget_amount, 2)

        txn_date = datetime(year, 1, 1) + timedelta(days=random.randint(0, 364))

        change_order_flag = random.random() < 0.08
        change_order_ref  = f"CO-{random.randint(1, 200):04d}" if change_order_flag else None

        period = f"{year}-{random.randint(1, 12):02d}"

        rows.append({
            'transaction_id':    txn_id,
            'project_id':        project_id,
            'vendor_id':         vendor_id,
            'po_id':             po_id,
            'contract_id':       contract_id,
            'invoice_id':        invoice_id,
            'wbs_code':          wbs_code,
            'cost_type':         cost_type,
            'cost_status':       cost_status,
            'budget_amount':     budget_amount,
            'actual_amount':     actual_amount,
            'variance':          variance,
            'currency':          random.choices(['SAR', 'USD', 'EUR'], [0.60, 0.30, 0.10])[0],
            'transaction_date':  txn_date.strftime('%Y-%m-%d'),
            'posting_date':      txn_date.strftime('%Y-%m-%d') if cost_status == 'POSTED' else None,
            'fiscal_period':     period,
            'change_order_flag': change_order_flag,
            'change_order_ref':  change_order_ref,
            'description':       fake.sentence(nb_words=5),
            'created_date':      txn_date.strftime('%Y-%m-%d'),
        })

    return pd.DataFrame(rows)


# ── Write to Excel ────────────────────────────────────────────────────────────
def main():
    print(f"  Generating {N:,} project actual records...", flush=True)
    df = generate_project_actuals(N)

    print(f"  Writing to {OUT_FILE}...", flush=True)
    with pd.ExcelWriter(OUT_FILE, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='project_actuals')
        meta = pd.DataFrame({
            'key':   ['source', 'records', 'generated_at', 'seed', 'schema_version'],
            'value': ['project_actuals.xlsx', str(len(df)),
                      datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                      str(SEED), '1.0']
        })
        meta.to_excel(writer, index=False, sheet_name='_metadata')

    size_mb = os.path.getsize(OUT_FILE) / 1_048_576
    print(f"  OUTPUT: {OUT_FILE}")
    print(f"  ROWS: {len(df)}")
    print(f"  SIZE: {size_mb:.1f} MB")

    # ── Validation ──
    assert len(df) == N, f"Expected {N} rows, got {len(df)}"
    assert df['transaction_id'].nunique() == N
    assert df['project_id'].isin(project_ids).all()
    # variance = actual - budget
    calc_var = (df['actual_amount'] - df['budget_amount']).round(2)
    assert (calc_var - df['variance']).abs().max() < 0.02, "Variance calculation mismatch"
    # POSTED should have invoice_id
    posted = df[df['cost_status'] == 'POSTED']
    assert posted['invoice_id'].notna().all(), "POSTED records must have invoice_id"
    print(f"  Validation: PASSED")


if __name__ == '__main__':
    main()