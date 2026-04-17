# Databricks notebook source
"""
Generator 06: PO Line Items
===============================
Output : data/raw/po_line_items.xlsx
Records: 100,000
Seed   : 42
Domain : Transactional — Procurement Detail

Business Rules:
  - FK → purchase_orders, materials
  - line_id = {po_id}-L{3-digit}
  - 1-20 lines per PO, sum ≈ 100K
  - CANCELLED PO → qty_received = 0
  - inspection_passed 92%
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
OUT_FILE  = os.path.join(OUT_DIR, 'po_line_items.xlsx')
SEED      = 42

fake = Faker()
Faker.seed(SEED)
np.random.seed(SEED)
random.seed(SEED)

os.makedirs(OUT_DIR, exist_ok=True)

# ── Load FK references ────────────────────────────────────────────────────────
po_df       = pd.read_excel(os.path.join(OUT_DIR, 'purchase_orders.xlsx'), sheet_name='purchase_orders')
materials_df = pd.read_excel(os.path.join(OUT_DIR, 'materials.xlsx'), sheet_name='materials')

po_ids        = po_df['po_id'].tolist()
po_status_map = dict(zip(po_df['po_id'], po_df['po_status']))
material_ids  = materials_df['material_id'].tolist()
mat_uom_map   = dict(zip(materials_df['material_id'], materials_df['unit_of_measure']))
mat_price_map = dict(zip(materials_df['material_id'], materials_df['unit_price']))


# ── Generation ────────────────────────────────────────────────────────────────
def generate_po_line_items(n):
    rows = []
    total = 0
    po_line_counter = {}  # track next line number per PO

    while total < n:
        po_id = random.choice(po_ids)
        po_status = po_status_map.get(po_id, 'ISSUED')
        n_lines = random.randint(1, min(20, n - total))
        start_line = po_line_counter.get(po_id, 0)

        for line_num in range(start_line + 1, start_line + n_lines + 1):
            if total >= n:
                break

            mat_id    = random.choice(material_ids)
            uom       = mat_uom_map.get(mat_id, 'NOS')
            base_price = mat_price_map.get(mat_id, 100.0)

            qty_ordered  = random.randint(1, 500)
            unit_price   = round(base_price * random.uniform(0.8, 1.2), 2)
            line_value   = round(qty_ordered * unit_price, 2)

            # CANCELLED PO → qty_received = 0
            if po_status == 'CANCELLED':
                qty_received = 0
                qty_accepted = 0
            else:
                qty_received = random.randint(0, qty_ordered)
                qty_accepted = random.randint(0, qty_received) if qty_received > 0 else 0

            inspection_passed = random.random() < 0.92 if qty_received > 0 else None

            delivery_date = (datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1825))).strftime('%Y-%m-%d')
            actual_delivery = None
            if qty_received > 0:
                days_delta = random.randint(-10, 30)  # can be early
                actual_delivery = (datetime.strptime(delivery_date, '%Y-%m-%d') + timedelta(days=days_delta)).strftime('%Y-%m-%d')

            line_id = f"{po_id}-L{line_num:03d}"

            rows.append({
                'line_id':            line_id,
                'po_id':              po_id,
                'line_number':        line_num,
                'material_id':        mat_id,
                'description':        fake.sentence(nb_words=6),
                'unit_of_measure':    uom,
                'qty_ordered':        qty_ordered,
                'qty_received':       qty_received,
                'qty_accepted':       qty_accepted,
                'unit_price':         unit_price,
                'line_value':         line_value,
                'discount_pct':       round(random.choice([0, 0, 0, 2, 5, 10]), 2),
                'tax_rate':           random.choice([0.00, 0.05, 0.10, 0.15]),
                'delivery_date':      delivery_date,
                'actual_delivery_date': actual_delivery,
                'inspection_passed':  inspection_passed,
                'warehouse_location': f"WH-{random.choice(['A','B','C','D'])}-{random.randint(1,50):02d}",
                'notes':              fake.sentence() if random.random() < 0.15 else None,
                'created_date':       delivery_date,
            })
            total += 1

        po_line_counter[po_id] = line_num  # remember last line number for this PO

    return pd.DataFrame(rows[:n])


# ── Write to Excel ────────────────────────────────────────────────────────────
def main():
    print(f"  Generating {N:,} PO line item records...", flush=True)
    df = generate_po_line_items(N)

    print(f"  Writing to {OUT_FILE}...", flush=True)
    with pd.ExcelWriter(OUT_FILE, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='po_line_items')
        meta = pd.DataFrame({
            'key':   ['source', 'records', 'generated_at', 'seed', 'schema_version'],
            'value': ['po_line_items.xlsx', str(len(df)),
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
    assert df['line_id'].nunique() == N, "Duplicate line_ids"
    assert df['po_id'].isin(po_ids).all(), "All po_ids must be valid"
    assert df['material_id'].isin(material_ids).all(), "All material_ids must be valid"
    # CANCELLED POs → qty_received = 0
    cancelled_pos = set(po_df[po_df['po_status'] == 'CANCELLED']['po_id'])
    cancelled_lines = df[df['po_id'].isin(cancelled_pos)]
    assert (cancelled_lines['qty_received'] == 0).all(), "CANCELLED PO lines must have qty_received=0"
    print(f"  Validation: PASSED")


if __name__ == '__main__':
    main()