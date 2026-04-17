# Databricks notebook source
"""
Generator 09: Invoices
========================
Output : data/raw/invoices.xlsx
Records: 100,000
Seed   : 42
Domain : Transactional — Financial

Business Rules:
  - FK → vendors, purchase_orders, contracts, projects, employees
  - invoice_id = INV-{YEAR}-{7digit}
  - type: MATERIAL 40%, SERVICE 20%, PROGRESS 30%, ADVANCE 10%
  - early_payment_discount 2% if paid_within_days ≤ 10
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
OUT_FILE  = os.path.join(OUT_DIR, 'invoices.xlsx')
SEED      = 42

fake = Faker()
Faker.seed(SEED)
np.random.seed(SEED)
random.seed(SEED)

os.makedirs(OUT_DIR, exist_ok=True)

# ── Load FK references ────────────────────────────────────────────────────────
vendors_df  = pd.read_excel(os.path.join(OUT_DIR, 'vendors.xlsx'), sheet_name='vendors')
po_df       = pd.read_excel(os.path.join(OUT_DIR, 'purchase_orders.xlsx'), sheet_name='purchase_orders')
contracts_df = pd.read_excel(os.path.join(OUT_DIR, 'contracts.xlsx'), sheet_name='contracts')
projects_df  = pd.read_excel(os.path.join(OUT_DIR, 'projects.xlsx'), sheet_name='projects')
employees_df = pd.read_excel(os.path.join(OUT_DIR, 'employees.xlsx'), sheet_name='employees')

vendor_ids   = vendors_df['vendor_id'].tolist()
po_ids       = po_df['po_id'].tolist()
contract_ids = contracts_df['contract_id'].tolist()
project_ids  = projects_df['project_id'].tolist()

fin_employees = employees_df[
    (employees_df['department'].isin(['FINANCE', 'PROCUREMENT'])) &
    (employees_df['active'] == True)
]
approver_ids = fin_employees['employee_id'].tolist() if len(fin_employees) > 0 else employees_df['employee_id'].tolist()

# ── Lookup tables ─────────────────────────────────────────────────────────────
INV_TYPES   = ['MATERIAL', 'SERVICE', 'PROGRESS', 'ADVANCE']
INV_TYPE_W  = [0.40, 0.20, 0.30, 0.10]

INV_STATUS  = ['SUBMITTED', 'UNDER_REVIEW', 'APPROVED', 'REJECTED', 'PAID', 'PARTIALLY_PAID', 'ON_HOLD']
INV_STATUS_W = [0.10, 0.10, 0.25, 0.05, 0.30, 0.10, 0.10]

PAYMENT_TERMS = ['NET30', 'NET45', 'NET60', 'NET90', 'IMMEDIATE', 'NET15']
CURRENCIES    = ['SAR', 'USD', 'EUR', 'GBP', 'AED']
CURRENCY_W    = [0.50, 0.25, 0.10, 0.05, 0.10]


# ── Generation ────────────────────────────────────────────────────────────────
def generate_invoices(n):
    rows = []
    used_ids = set()

    values = np.random.lognormal(mean=11.5, sigma=1.5, size=n)
    values = np.clip(values, 500, 20_000_000)

    for i in range(n):
        year = random.randint(2019, 2025)
        seq  = i + 1
        inv_id = f"INV-{year}-{seq:07d}"

        inv_type = random.choices(INV_TYPES, INV_TYPE_W)[0]
        status   = random.choices(INV_STATUS, INV_STATUS_W)[0]

        vendor_id   = random.choice(vendor_ids)
        project_id  = random.choice(project_ids)
        approver_id = random.choice(approver_ids)

        # Link to PO or Contract depending on type
        if inv_type in ('MATERIAL', 'ADVANCE'):
            po_id = random.choice(po_ids)
            contract_id = None
        elif inv_type == 'PROGRESS':
            po_id = None
            contract_id = random.choice(contract_ids)
        else:  # SERVICE
            if random.random() < 0.5:
                po_id = random.choice(po_ids)
                contract_id = None
            else:
                po_id = None
                contract_id = random.choice(contract_ids)

        gross_amount = round(float(values[i]), 2)
        tax_rate     = random.choice([0.0, 0.05, 0.15])
        tax_amount   = round(gross_amount * tax_rate, 2)

        # Retention on progress invoices
        retention_pct = random.choice([0.05, 0.10]) if inv_type == 'PROGRESS' else 0.0
        retention_amt = round(gross_amount * retention_pct, 2)

        net_amount = round(gross_amount + tax_amount - retention_amt, 2)

        invoice_date = datetime(year, 1, 1) + timedelta(days=random.randint(0, 364))
        due_days     = random.choice([15, 30, 45, 60, 90])
        due_date     = invoice_date + timedelta(days=due_days)

        payment_term = random.choice(PAYMENT_TERMS)
        currency     = random.choices(CURRENCIES, CURRENCY_W)[0]

        # Early payment discount: 2% if paid within 10 days
        paid_within  = random.randint(1, due_days) if status in ('PAID', 'PARTIALLY_PAID') else None
        early_discount = round(gross_amount * 0.02, 2) if (paid_within and paid_within <= 10) else 0.0

        paid_date = (invoice_date + timedelta(days=paid_within)) if paid_within else None
        paid_amount = round(net_amount - early_discount, 2) if status == 'PAID' else (
            round(net_amount * random.uniform(0.3, 0.8), 2) if status == 'PARTIALLY_PAID' else None
        )

        rows.append({
            'invoice_id':        inv_id,
            'vendor_id':         vendor_id,
            'po_id':             po_id,
            'contract_id':       contract_id,
            'project_id':        project_id,
            'invoice_type':      inv_type,
            'invoice_status':    status,
            'invoice_date':      invoice_date.strftime('%Y-%m-%d'),
            'due_date':          due_date.strftime('%Y-%m-%d'),
            'gross_amount':      gross_amount,
            'tax_rate':          tax_rate,
            'tax_amount':        tax_amount,
            'retention_pct':     retention_pct,
            'retention_amount':  retention_amt,
            'net_amount':        net_amount,
            'currency':          currency,
            'payment_term':      payment_term,
            'early_payment_discount': early_discount,
            'paid_date':         paid_date.strftime('%Y-%m-%d') if paid_date else None,
            'paid_amount':       paid_amount,
            'approved_by':       approver_id if status in ('APPROVED', 'PAID', 'PARTIALLY_PAID') else None,
            'approval_date':     (invoice_date + timedelta(days=random.randint(1, 14))).strftime('%Y-%m-%d') if status in ('APPROVED', 'PAID', 'PARTIALLY_PAID') else None,
            'invoice_ref':       f"INV-REF-{fake.bothify('??###??').upper()}",
            'description':       fake.sentence(nb_words=6),
            'created_date':      invoice_date.strftime('%Y-%m-%d'),
        })

    return pd.DataFrame(rows)


# ── Write to Excel ────────────────────────────────────────────────────────────
def main():
    print(f"  Generating {N:,} invoice records...", flush=True)
    df = generate_invoices(N)

    print(f"  Writing to {OUT_FILE}...", flush=True)
    with pd.ExcelWriter(OUT_FILE, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='invoices')
        meta = pd.DataFrame({
            'key':   ['source', 'records', 'generated_at', 'seed', 'schema_version'],
            'value': ['invoices.xlsx', str(len(df)),
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
    assert df['invoice_id'].nunique() == N
    assert df['vendor_id'].isin(vendor_ids).all()
    assert df['project_id'].isin(project_ids).all()
    # Early discount only for fast-paid
    disc_rows = df[df['early_payment_discount'] > 0]
    if len(disc_rows) > 0:
        assert disc_rows['invoice_status'].isin(['PAID', 'PARTIALLY_PAID']).all()
    print(f"  Validation: PASSED")


if __name__ == '__main__':
    main()