# Databricks notebook source
"""
Generator 10: Goods Receipts
==============================
Output : data/raw/goods_receipts.xlsx
Records: 100,000
Seed   : 42
Domain : Transactional — Warehouse / Logistics

Business Rules:
  - FK → purchase_orders, po_line_items, vendors, materials, projects
  - grn_id = GRN-{YEAR}-{7digit}
  - qty_accepted ≤ qty_delivered
  - inspection_certificate = CERT-{8digit}
  - storage_location = {site}-WAREHOUSE-{section}
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
OUT_FILE  = os.path.join(OUT_DIR, 'goods_receipts.xlsx')
SEED      = 42

fake = Faker()
Faker.seed(SEED)
np.random.seed(SEED)
random.seed(SEED)

os.makedirs(OUT_DIR, exist_ok=True)

# ── Load FK references ────────────────────────────────────────────────────────
po_df       = pd.read_excel(os.path.join(OUT_DIR, 'purchase_orders.xlsx'), sheet_name='purchase_orders')
line_df     = pd.read_excel(os.path.join(OUT_DIR, 'po_line_items.xlsx'), sheet_name='po_line_items')
vendors_df  = pd.read_excel(os.path.join(OUT_DIR, 'vendors.xlsx'), sheet_name='vendors')
materials_df = pd.read_excel(os.path.join(OUT_DIR, 'materials.xlsx'), sheet_name='materials')
projects_df  = pd.read_excel(os.path.join(OUT_DIR, 'projects.xlsx'), sheet_name='projects')

po_ids       = po_df['po_id'].tolist()
po_vendor_map = dict(zip(po_df['po_id'], po_df['vendor_id']))
po_project_map = dict(zip(po_df['po_id'], po_df['project_id']))
line_item_ids = line_df['line_id'].tolist()
line_po_map   = dict(zip(line_df['line_id'], line_df['po_id']))
line_mat_map  = dict(zip(line_df['line_id'], line_df['material_id']))
vendor_ids    = vendors_df['vendor_id'].tolist()
material_ids  = materials_df['material_id'].tolist()
project_ids   = projects_df['project_id'].tolist()

# ── Lookup tables ─────────────────────────────────────────────────────────────
GRN_STATUS  = ['RECEIVED', 'INSPECTED', 'ACCEPTED', 'PARTIALLY_ACCEPTED', 'REJECTED', 'RETURNED']
GRN_STATUS_W = [0.15, 0.15, 0.40, 0.15, 0.10, 0.05]

SITES = ['SITE-A', 'SITE-B', 'SITE-C', 'SITE-D', 'SITE-E']
WAREHOUSE_SECTIONS = ['SEC-01', 'SEC-02', 'SEC-03', 'SEC-04', 'SEC-05', 'SEC-06']

INSPECTION_RESULT = ['PASSED', 'FAILED', 'CONDITIONAL']
INSPECTION_RESULT_W = [0.85, 0.08, 0.07]


# ── Generation ────────────────────────────────────────────────────────────────
def generate_goods_receipts(n):
    rows = []

    for i in range(n):
        year = random.randint(2019, 2025)
        grn_id = f"GRN-{year}-{i+1:07d}"

        line_item_id = random.choice(line_item_ids)
        po_id      = line_po_map.get(line_item_id, random.choice(po_ids))
        vendor_id  = po_vendor_map.get(po_id, random.choice(vendor_ids))
        project_id = po_project_map.get(po_id, random.choice(project_ids))
        mat_id     = line_mat_map.get(line_item_id, random.choice(material_ids))

        status       = random.choices(GRN_STATUS, GRN_STATUS_W)[0]
        qty_delivered = random.randint(1, 500)

        # qty_accepted ≤ qty_delivered
        if status == 'REJECTED':
            qty_accepted = 0
        elif status == 'PARTIALLY_ACCEPTED':
            qty_accepted = random.randint(1, max(1, qty_delivered - 1))
        elif status in ('ACCEPTED', 'INSPECTED', 'RECEIVED'):
            qty_accepted = qty_delivered
        else:  # RETURNED
            qty_accepted = 0

        qty_rejected = qty_delivered - qty_accepted

        receipt_date  = datetime(year, 1, 1) + timedelta(days=random.randint(0, 364))
        delivery_note = f"DN-{fake.bothify('###???').upper()}"

        site    = random.choice(SITES)
        section = random.choice(WAREHOUSE_SECTIONS)
        storage_location = f"{site}-WAREHOUSE-{section}"

        inspection = random.choices(INSPECTION_RESULT, INSPECTION_RESULT_W)[0]
        if status == 'REJECTED':
            inspection = 'FAILED'
        elif status in ('ACCEPTED',):
            inspection = 'PASSED'

        cert_id = f"CERT-{random.randint(10000000, 99999999)}" if inspection != 'FAILED' else None

        rows.append({
            'grn_id':               grn_id,
            'po_id':                po_id,
            'line_item_id':         line_item_id,
            'vendor_id':            vendor_id,
            'project_id':           project_id,
            'material_id':          mat_id,
            'receipt_date':         receipt_date.strftime('%Y-%m-%d'),
            'qty_delivered':        qty_delivered,
            'qty_accepted':         qty_accepted,
            'qty_rejected':         qty_rejected,
            'grn_status':           status,
            'inspection_result':    inspection,
            'inspection_certificate': cert_id,
            'delivery_note_ref':    delivery_note,
            'storage_location':     storage_location,
            'received_by':          fake.name(),
            'remarks':              fake.sentence(nb_words=5) if random.random() < 0.15 else None,
            'damage_flag':          random.random() < 0.05,
            'temperature_check':    random.random() < 0.10,
            'created_date':         receipt_date.strftime('%Y-%m-%d'),
        })

    return pd.DataFrame(rows)


# ── Write to Excel ────────────────────────────────────────────────────────────
def main():
    print(f"  Generating {N:,} goods receipt records...", flush=True)
    df = generate_goods_receipts(N)

    print(f"  Writing to {OUT_FILE}...", flush=True)
    with pd.ExcelWriter(OUT_FILE, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='goods_receipts')
        meta = pd.DataFrame({
            'key':   ['source', 'records', 'generated_at', 'seed', 'schema_version'],
            'value': ['goods_receipts.xlsx', str(len(df)),
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
    assert df['grn_id'].nunique() == N
    # qty_accepted ≤ qty_delivered
    assert (df['qty_accepted'] <= df['qty_delivered']).all(), "qty_accepted > qty_delivered found"
    # REJECTED → qty_accepted = 0
    rejected = df[df['grn_status'] == 'REJECTED']
    assert (rejected['qty_accepted'] == 0).all(), "REJECTED should have qty_accepted=0"
    print(f"  Validation: PASSED")


if __name__ == '__main__':
    main()