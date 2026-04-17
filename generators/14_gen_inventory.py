# Databricks notebook source
"""
Generator 14: Inventory Movements
====================================
Output : data/raw/inventory.xlsx
Records: 100,000
Seed   : 42
Domain : Transactional — Warehouse

Business Rules:
  - FK → materials, projects, goods_receipts
  - movement_id = INV-MOV-{8digit}
  - movement_type: RECEIPT / ISSUE / RETURN / TRANSFER / ADJUSTMENT / WRITEOFF
  - qty positive for RECEIPT/RETURN, negative for ISSUE/WRITEOFF
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
OUT_FILE  = os.path.join(OUT_DIR, 'inventory.xlsx')
SEED      = 42

fake = Faker()
Faker.seed(SEED)
np.random.seed(SEED)
random.seed(SEED)

os.makedirs(OUT_DIR, exist_ok=True)

# ── Load FK references ────────────────────────────────────────────────────────
materials_df = pd.read_excel(os.path.join(OUT_DIR, 'materials.xlsx'), sheet_name='materials')
projects_df  = pd.read_excel(os.path.join(OUT_DIR, 'projects.xlsx'), sheet_name='projects')
grn_df       = pd.read_excel(os.path.join(OUT_DIR, 'goods_receipts.xlsx'), sheet_name='goods_receipts')

material_ids  = materials_df['material_id'].tolist()
mat_uom_map   = dict(zip(materials_df['material_id'], materials_df['unit_of_measure']))
mat_price_map = dict(zip(materials_df['material_id'], materials_df['unit_price']))
project_ids   = projects_df['project_id'].tolist()
grn_ids       = grn_df['grn_id'].tolist()

# ── Lookup tables ─────────────────────────────────────────────────────────────
MOVEMENT_TYPES = ['RECEIPT', 'ISSUE', 'RETURN', 'TRANSFER', 'ADJUSTMENT', 'WRITEOFF']
MOVEMENT_TYPE_W = [0.30, 0.35, 0.08, 0.12, 0.10, 0.05]

POSITIVE_TYPES = {'RECEIPT', 'RETURN'}
NEGATIVE_TYPES = {'ISSUE', 'WRITEOFF'}
# TRANSFER and ADJUSTMENT can be positive or negative

SITES = ['SITE-A', 'SITE-B', 'SITE-C', 'SITE-D', 'SITE-E']
WAREHOUSE_SECTIONS = ['SEC-01', 'SEC-02', 'SEC-03', 'SEC-04', 'SEC-05', 'SEC-06']

REASON_CODES = {
    'RECEIPT':    ['PO_DELIVERY', 'RETURN_FROM_SITE', 'TRANSFER_IN'],
    'ISSUE':      ['PROJECT_CONSUMPTION', 'MAINTENANCE', 'PRODUCTION'],
    'RETURN':     ['EXCESS_MATERIAL', 'DEFECTIVE', 'WRONG_DELIVERY'],
    'TRANSFER':   ['SITE_TRANSFER', 'WAREHOUSE_REORG', 'BUFFER_STOCK'],
    'ADJUSTMENT': ['STOCK_COUNT', 'SYSTEM_CORRECTION', 'QUALITY_HOLD'],
    'WRITEOFF':   ['EXPIRED', 'DAMAGED', 'OBSOLETE', 'THEFT'],
}


# ── Generation ────────────────────────────────────────────────────────────────
def generate_inventory(n):
    rows = []

    for i in range(n):
        mov_id = f"INV-MOV-{i+1:08d}"

        mat_id    = random.choice(material_ids)
        uom       = mat_uom_map.get(mat_id, 'NOS')
        unit_cost = mat_price_map.get(mat_id, 100.0)

        project_id = random.choice(project_ids)
        mov_type   = random.choices(MOVEMENT_TYPES, MOVEMENT_TYPE_W)[0]

        base_qty = random.randint(1, 500)
        if mov_type in POSITIVE_TYPES:
            quantity = base_qty
        elif mov_type in NEGATIVE_TYPES:
            quantity = -base_qty
        else:  # TRANSFER or ADJUSTMENT
            quantity = base_qty if random.random() < 0.5 else -base_qty

        total_value = round(abs(quantity) * unit_cost, 2)
        if quantity < 0:
            total_value = -total_value

        grn_id = random.choice(grn_ids) if mov_type == 'RECEIPT' else None

        site    = random.choice(SITES)
        section = random.choice(WAREHOUSE_SECTIONS)
        from_location = f"{site}-WAREHOUSE-{section}"

        # Transfer has a different to_location
        if mov_type == 'TRANSFER':
            to_site    = random.choice([s for s in SITES if s != site] or SITES)
            to_section = random.choice(WAREHOUSE_SECTIONS)
            to_location = f"{to_site}-WAREHOUSE-{to_section}"
        else:
            to_location = None

        year = random.randint(2019, 2025)
        mov_date = datetime(year, 1, 1) + timedelta(days=random.randint(0, 364))

        batch_no = f"BATCH-{fake.bothify('##??###').upper()}" if random.random() < 0.40 else None
        reason   = random.choice(REASON_CODES.get(mov_type, ['OTHER']))

        rows.append({
            'movement_id':     mov_id,
            'material_id':     mat_id,
            'project_id':      project_id,
            'grn_id':          grn_id,
            'movement_type':   mov_type,
            'quantity':         quantity,
            'unit_of_measure': uom,
            'unit_cost':       unit_cost,
            'total_value':     total_value,
            'from_location':   from_location,
            'to_location':     to_location,
            'batch_number':    batch_no,
            'reason_code':     reason,
            'movement_date':   mov_date.strftime('%Y-%m-%d'),
            'posted_by':       fake.name(),
            'approved_by':     fake.name() if mov_type in ('WRITEOFF', 'ADJUSTMENT') else None,
            'remarks':         fake.sentence(nb_words=4) if random.random() < 0.10 else None,
            'created_date':    mov_date.strftime('%Y-%m-%d'),
        })

    return pd.DataFrame(rows)


# ── Write to Excel ────────────────────────────────────────────────────────────
def main():
    print(f"  Generating {N:,} inventory movement records...", flush=True)
    df = generate_inventory(N)

    print(f"  Writing to {OUT_FILE}...", flush=True)
    with pd.ExcelWriter(OUT_FILE, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='inventory')
        meta = pd.DataFrame({
            'key':   ['source', 'records', 'generated_at', 'seed', 'schema_version'],
            'value': ['inventory.xlsx', str(len(df)),
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
    assert df['movement_id'].nunique() == N
    assert df['material_id'].isin(material_ids).all()
    assert df['project_id'].isin(project_ids).all()
    # RECEIPT/RETURN → positive qty
    pos = df[df['movement_type'].isin(['RECEIPT', 'RETURN'])]
    assert (pos['quantity'] > 0).all(), "RECEIPT/RETURN should have positive qty"
    # ISSUE/WRITEOFF → negative qty
    neg = df[df['movement_type'].isin(['ISSUE', 'WRITEOFF'])]
    assert (neg['quantity'] < 0).all(), "ISSUE/WRITEOFF should have negative qty"
    print(f"  Validation: PASSED")


if __name__ == '__main__':
    main()