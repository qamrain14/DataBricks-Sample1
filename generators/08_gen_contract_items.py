# Databricks notebook source
"""
Generator 08: Contract Items (BOQ)
====================================
Output : data/raw/contract_items.xlsx
Records: 100,000
Seed   : 42
Domain : Transactional — Contract Detail

Business Rules:
  - FK → contracts, materials
  - item_id = {contract_id}-BOQ-{4digit}
  - boq_reference = section.subsection.item format
  - milestone_flag 15%
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
OUT_FILE  = os.path.join(OUT_DIR, 'contract_items.xlsx')
SEED      = 42

fake = Faker()
Faker.seed(SEED)
np.random.seed(SEED)
random.seed(SEED)

os.makedirs(OUT_DIR, exist_ok=True)

# ── Load FK references ────────────────────────────────────────────────────────
contracts_df = pd.read_excel(os.path.join(OUT_DIR, 'contracts.xlsx'), sheet_name='contracts')
materials_df = pd.read_excel(os.path.join(OUT_DIR, 'materials.xlsx'), sheet_name='materials')

contract_ids   = contracts_df['contract_id'].tolist()
ctr_type_map   = dict(zip(contracts_df['contract_id'], contracts_df['contract_type']))
material_ids   = materials_df['material_id'].tolist()
mat_uom_map    = dict(zip(materials_df['material_id'], materials_df['unit_of_measure']))
mat_price_map  = dict(zip(materials_df['material_id'], materials_df['unit_price']))

# ── Lookup tables ─────────────────────────────────────────────────────────────
BOQ_SECTIONS = [
    'General Requirements', 'Earthworks', 'Concrete Works', 'Steel Structures',
    'Mechanical Works', 'Electrical Works', 'Instrumentation', 'Piping',
    'Painting & Coatings', 'Insulation', 'Civil Works', 'Commissioning',
]

ITEM_STATUS = ['ACTIVE', 'REVISED', 'DELETED', 'ON_HOLD']
ITEM_STATUS_W = [0.75, 0.10, 0.05, 0.10]


# ── Generation ────────────────────────────────────────────────────────────────
def generate_contract_items(n):
    rows = []
    total = 0

    while total < n:
        ctr_id  = random.choice(contract_ids)
        n_items = random.randint(1, min(15, n - total))

        section = random.choice(BOQ_SECTIONS)
        section_num = random.randint(1, 12)

        for item_num in range(1, n_items + 1):
            if total >= n:
                break

            mat_id = random.choice(material_ids)
            uom    = mat_uom_map.get(mat_id, 'NOS')
            base_price = mat_price_map.get(mat_id, 100.0)

            item_id = f"{ctr_id}-BOQ-{total+1:04d}"
            boq_ref = f"{section_num}.{random.randint(1, 10)}.{item_num}"

            quantity = random.randint(1, 1000)
            unit_rate = round(base_price * random.uniform(0.9, 1.5), 2)
            amount = round(quantity * unit_rate, 2)

            qty_executed = random.randint(0, quantity)
            executed_pct = round((qty_executed / quantity) * 100, 1) if quantity > 0 else 0

            milestone_flag = random.random() < 0.15
            milestone_desc = fake.sentence(nb_words=5) if milestone_flag else None

            status = random.choices(ITEM_STATUS, ITEM_STATUS_W)[0]

            rows.append({
                'item_id':            item_id,
                'contract_id':        ctr_id,
                'boq_reference':      boq_ref,
                'boq_section':        section,
                'line_number':        item_num,
                'material_id':        mat_id,
                'description':        fake.sentence(nb_words=6),
                'unit_of_measure':    uom,
                'quantity':           quantity,
                'unit_rate':          unit_rate,
                'amount':             amount,
                'qty_executed':       qty_executed,
                'executed_pct':       executed_pct,
                'milestone_flag':     milestone_flag,
                'milestone_description': milestone_desc,
                'item_status':        status,
                'variation_ref':      f"VO-{random.randint(1, 50):03d}" if random.random() < 0.10 else None,
                'retention_applicable': random.random() < 0.70,
                'notes':              fake.sentence() if random.random() < 0.10 else None,
                'created_date':       (datetime(2019, 1, 1) + timedelta(days=random.randint(0, 2190))).strftime('%Y-%m-%d'),
            })
            total += 1

    return pd.DataFrame(rows[:n])


# ── Write to Excel ────────────────────────────────────────────────────────────
def main():
    print(f"  Generating {N:,} contract item records...", flush=True)
    df = generate_contract_items(N)

    print(f"  Writing to {OUT_FILE}...", flush=True)
    with pd.ExcelWriter(OUT_FILE, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='contract_items')
        meta = pd.DataFrame({
            'key':   ['source', 'records', 'generated_at', 'seed', 'schema_version'],
            'value': ['contract_items.xlsx', str(len(df)),
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
    assert df['item_id'].nunique() == N, "Duplicate item_ids"
    assert df['contract_id'].isin(contract_ids).all()
    assert df['material_id'].isin(material_ids).all()
    # milestone_flag should be ~15%
    mf_pct = df['milestone_flag'].mean()
    assert 0.10 < mf_pct < 0.20, f"milestone_flag ratio {mf_pct:.2%} out of range"
    print(f"  Validation: PASSED")


if __name__ == '__main__':
    main()