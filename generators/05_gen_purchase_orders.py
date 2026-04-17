# Databricks notebook source
"""
Generator 05: Purchase Orders
===============================
Output : data/raw/purchase_orders.xlsx
Records: 100,000
Seed   : 42
Domain : Transactional — Procurement

Business Rules:
  - FK → vendors (APPROVED only), projects
  - PO types: STANDARD(50%), BLANKET(20%), EMERGENCY(10%), FRAMEWORK(20%)
  - EMERGENCY → always CRITICAL priority
  - Currency: USD 60%, EUR 20%, GBP 10%, SAR 10%
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
OUT_FILE  = os.path.join(OUT_DIR, 'purchase_orders.xlsx')
SEED      = 42

fake = Faker()
Faker.seed(SEED)
np.random.seed(SEED)
random.seed(SEED)

os.makedirs(OUT_DIR, exist_ok=True)

# ── Load FK references ────────────────────────────────────────────────────────
vendors_df  = pd.read_excel(os.path.join(OUT_DIR, 'vendors.xlsx'), sheet_name='vendors')
projects_df = pd.read_excel(os.path.join(OUT_DIR, 'projects.xlsx'), sheet_name='projects')

# Only APPROVED vendors
approved_vendors = vendors_df[vendors_df['prequalification_status'] == 'APPROVED']['vendor_id'].tolist()
project_ids      = projects_df['project_id'].tolist()

# ── Lookup tables ─────────────────────────────────────────────────────────────
PO_TYPES   = ['STANDARD', 'BLANKET', 'EMERGENCY', 'FRAMEWORK']
PO_TYPE_W  = [0.50, 0.20, 0.10, 0.20]

PO_STATUS_LIST = ['DRAFT', 'PENDING_APPROVAL', 'APPROVED', 'ISSUED', 'PARTIALLY_RECEIVED', 'COMPLETED', 'CANCELLED']
PO_STATUS_W    = [0.05, 0.08, 0.12, 0.25, 0.20, 0.25, 0.05]

PRIORITY_LIST = ['NORMAL', 'URGENT', 'CRITICAL']
PRIORITY_W    = [0.70, 0.20, 0.10]

CURRENCIES = ['USD', 'EUR', 'GBP', 'SAR']
CURRENCY_W = [0.60, 0.20, 0.10, 0.10]

PAYMENT_TERMS = ['NET-30', 'NET-60', 'NET-90', 'ADVANCE-30%', 'LC-AT-SIGHT', 'LC-90-DAYS']
PAYMENT_TERMS_W = [0.30, 0.25, 0.15, 0.10, 0.10, 0.10]

DELIVERY_TERMS = ['EXW', 'FOB', 'CIF', 'DDP', 'FCA', 'DAP']
DELIVERY_TERMS_W = [0.10, 0.25, 0.25, 0.15, 0.15, 0.10]


# ── Generation ────────────────────────────────────────────────────────────────
def generate_purchase_orders(n):
    rows = []
    po_date_start = datetime(2019, 1, 1)
    po_date_range = (datetime(2024, 12, 31) - po_date_start).days

    for i in range(n):
        year    = random.randint(2019, 2024)
        po_id   = f"PO-{year}-{i+1:07d}"
        po_type = random.choices(PO_TYPES, PO_TYPE_W)[0]

        # EMERGENCY → CRITICAL priority
        if po_type == 'EMERGENCY':
            priority = 'CRITICAL'
        else:
            priority = random.choices(PRIORITY_LIST, PRIORITY_W)[0]

        status   = random.choices(PO_STATUS_LIST, PO_STATUS_W)[0]
        currency = random.choices(CURRENCIES, CURRENCY_W)[0]

        vendor_id  = random.choice(approved_vendors)
        project_id = random.choice(project_ids)

        # PO value: lognormal with median ~250K
        po_value = round(float(np.random.lognormal(mean=12.4, sigma=1.2)), 2)
        po_value = max(1000, min(po_value, 50_000_000))

        # Tax rate
        tax_rate = random.choice([0.00, 0.05, 0.10, 0.15])
        tax_amount = round(po_value * tax_rate, 2)
        total_value = round(po_value + tax_amount, 2)

        po_date = (po_date_start + timedelta(days=random.randint(0, po_date_range))).strftime('%Y-%m-%d')
        delivery_date = (datetime.strptime(po_date, '%Y-%m-%d') + timedelta(days=random.randint(14, 180))).strftime('%Y-%m-%d')

        approval_date = None
        if status not in ('DRAFT', 'PENDING_APPROVAL'):
            approval_date = (datetime.strptime(po_date, '%Y-%m-%d') + timedelta(days=random.randint(1, 14))).strftime('%Y-%m-%d')

        payment_terms  = random.choices(PAYMENT_TERMS, PAYMENT_TERMS_W)[0]
        delivery_terms = random.choices(DELIVERY_TERMS, DELIVERY_TERMS_W)[0]

        rows.append({
            'po_id':            po_id,
            'po_type':          po_type,
            'po_status':        status,
            'priority':         priority,
            'vendor_id':        vendor_id,
            'project_id':       project_id,
            'po_date':          po_date,
            'delivery_date':    delivery_date,
            'approval_date':    approval_date,
            'po_value':         po_value,
            'tax_rate':         tax_rate,
            'tax_amount':       tax_amount,
            'total_value':      total_value,
            'currency':         currency,
            'payment_terms':    payment_terms,
            'delivery_terms':   delivery_terms,
            'description':      fake.sentence(nb_words=8),
            'buyer_name':       fake.name(),
            'requisition_ref':  f"REQ-{year}-{random.randint(100000, 999999)}",
            'ship_to_address':  fake.address().replace('\n', ', '),
            'notes':            fake.sentence() if random.random() < 0.30 else None,
            'created_date':     po_date,
        })

    return pd.DataFrame(rows)


# ── Write to Excel ────────────────────────────────────────────────────────────
def main():
    print(f"  Generating {N:,} purchase order records...", flush=True)
    df = generate_purchase_orders(N)

    print(f"  Writing to {OUT_FILE}...", flush=True)
    with pd.ExcelWriter(OUT_FILE, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='purchase_orders')
        meta = pd.DataFrame({
            'key':   ['source', 'records', 'generated_at', 'seed', 'schema_version'],
            'value': ['purchase_orders.xlsx', str(len(df)),
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
    assert df['po_id'].nunique() == N, "Duplicate po_ids"
    assert df['po_type'].isin(PO_TYPES).all()
    # EMERGENCY → CRITICAL
    emer = df[df['po_type'] == 'EMERGENCY']
    assert (emer['priority'] == 'CRITICAL').all(), "EMERGENCY POs must be CRITICAL priority"
    # All vendor refs must be approved vendors
    assert df['vendor_id'].isin(approved_vendors).all(), "All vendors must be APPROVED"
    assert df['project_id'].isin(project_ids).all(), "All project_ids must be valid"
    print(f"  Validation: PASSED")


if __name__ == '__main__':
    main()