# Databricks notebook source
"""
Generator 07: Contracts
===============================
Output : data/raw/contracts.xlsx
Records: 100,000
Seed   : 42
Domain : Transactional — Contracts

Business Rules:
  - FK → vendors, projects, employees (contract_manager)
  - Types: LUMP_SUM(40%), REMEASURE(30%), COST_PLUS(15%), TURNKEY(10%), EPC(5%)
  - original_value lognormal median=1M range=50K-500M
  - revised_value = original * random(0.8, 1.3)
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
OUT_FILE  = os.path.join(OUT_DIR, 'contracts.xlsx')
SEED      = 42

fake = Faker()
Faker.seed(SEED)
np.random.seed(SEED)
random.seed(SEED)

os.makedirs(OUT_DIR, exist_ok=True)

# ── Load FK references ────────────────────────────────────────────────────────
vendors_df   = pd.read_excel(os.path.join(OUT_DIR, 'vendors.xlsx'), sheet_name='vendors')
projects_df  = pd.read_excel(os.path.join(OUT_DIR, 'projects.xlsx'), sheet_name='projects')
employees_df = pd.read_excel(os.path.join(OUT_DIR, 'employees.xlsx'), sheet_name='employees')

vendor_ids  = vendors_df[vendors_df['prequalification_status'] == 'APPROVED']['vendor_id'].tolist()
project_ids = projects_df['project_id'].tolist()
# Contract managers from CONTRACTS or PROCUREMENT departments
mgr_mask    = employees_df['department'].isin(['CONTRACTS', 'PROCUREMENT']) & employees_df['active']
manager_ids = employees_df[mgr_mask]['employee_id'].tolist()
if not manager_ids:
    manager_ids = employees_df['employee_id'].tolist()

# ── Lookup tables ─────────────────────────────────────────────────────────────
CONTRACT_TYPES = ['LUMP_SUM', 'REMEASURE', 'COST_PLUS', 'TURNKEY', 'EPC']
TYPE_W         = [0.40, 0.30, 0.15, 0.10, 0.05]

STATUS_LIST = ['DRAFT', 'UNDER_NEGOTIATION', 'APPROVED', 'ACTIVE', 'SUSPENDED', 'COMPLETED', 'TERMINATED']
STATUS_W    = [0.05, 0.08, 0.10, 0.35, 0.05, 0.30, 0.07]

SCOPE_OF_WORK = [
    'Civil Works', 'Mechanical Installation', 'Electrical & Instrumentation',
    'Piping & Fabrication', 'HVAC Installation', 'Fire Fighting Systems',
    'Structural Steel Erection', 'Road & Infrastructure', 'Painting & Coatings',
    'Insulation Works', 'Commissioning Support', 'Engineering Design',
    'Scaffolding Services', 'Transportation & Logistics', 'Catering & Camp Services',
]


# ── Generation ────────────────────────────────────────────────────────────────
def generate_contracts(n):
    rows = []

    for i in range(n):
        year = random.randint(2019, 2024)
        ctr_id = f"CTR-{year}-{i+1:06d}"
        ctr_type = random.choices(CONTRACT_TYPES, TYPE_W)[0]
        status   = random.choices(STATUS_LIST, STATUS_W)[0]

        vendor_id  = random.choice(vendor_ids)
        project_id = random.choice(project_ids)
        manager_id = random.choice(manager_ids)

        # original_value: lognormal median ~1M (ln(1M) ≈ 13.82)
        orig_val = float(np.random.lognormal(mean=13.82, sigma=1.5))
        orig_val = round(max(50_000, min(orig_val, 500_000_000)), 2)

        # revised = original * (0.8 to 1.3)
        revision_factor = round(random.uniform(0.8, 1.3), 4)
        revised_val = round(orig_val * revision_factor, 2)

        variation_orders = max(0, round((revision_factor - 1.0) * random.randint(1, 10)))

        currency = random.choices(['USD', 'EUR', 'GBP', 'SAR'], [0.55, 0.20, 0.10, 0.15])[0]

        start_date = datetime(year, random.randint(1, 12), random.randint(1, 28))
        duration_months = random.randint(3, 60)
        end_date = start_date + timedelta(days=duration_months * 30)

        retention_pct = random.choice([0, 5, 10])
        performance_bond_pct = random.choice([0, 5, 10, 15])
        advance_payment_pct = random.choice([0, 10, 15, 20, 30])
        liquidated_damages_pct = random.choice([0.5, 1.0, 2.0, 5.0])

        rows.append({
            'contract_id':             ctr_id,
            'contract_type':           ctr_type,
            'contract_status':         status,
            'vendor_id':               vendor_id,
            'project_id':              project_id,
            'contract_manager_id':     manager_id,
            'scope_of_work':           random.choice(SCOPE_OF_WORK),
            'original_value':          orig_val,
            'revised_value':           revised_val,
            'revision_factor':         revision_factor,
            'variation_orders':        variation_orders,
            'currency':                currency,
            'start_date':              start_date.strftime('%Y-%m-%d'),
            'end_date':                end_date.strftime('%Y-%m-%d'),
            'duration_months':         duration_months,
            'retention_pct':           retention_pct,
            'performance_bond_pct':    performance_bond_pct,
            'advance_payment_pct':     advance_payment_pct,
            'liquidated_damages_pct':  liquidated_damages_pct,
            'payment_terms':           random.choice(['MONTHLY', 'MILESTONE', 'PROGRESS', 'COMPLETION']),
            'insurance_required':      random.random() < 0.70,
            'description':             fake.sentence(nb_words=10),
            'created_date':            start_date.strftime('%Y-%m-%d'),
        })

    return pd.DataFrame(rows)


# ── Write to Excel ────────────────────────────────────────────────────────────
def main():
    print(f"  Generating {N:,} contract records...", flush=True)
    df = generate_contracts(N)

    print(f"  Writing to {OUT_FILE}...", flush=True)
    with pd.ExcelWriter(OUT_FILE, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='contracts')
        meta = pd.DataFrame({
            'key':   ['source', 'records', 'generated_at', 'seed', 'schema_version'],
            'value': ['contracts.xlsx', str(len(df)),
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
    assert df['contract_id'].nunique() == N, "Duplicate contract_ids"
    assert df['contract_type'].isin(CONTRACT_TYPES).all()
    assert df['vendor_id'].isin(vendor_ids).all(), "All vendor_ids must be valid APPROVED vendors"
    assert df['project_id'].isin(project_ids).all()
    assert (df['original_value'] >= 50_000).all(), "original_value must be >= 50K"
    assert (df['original_value'] <= 500_000_000).all(), "original_value must be <= 500M"
    print(f"  Validation: PASSED")


if __name__ == '__main__':
    main()