# Databricks notebook source
"""
Generator 11: Project Budgets
================================
Output : data/raw/project_budgets.xlsx
Records: 100,000
Seed   : 42
Domain : Financial — Budgeting

Business Rules:
  - FK → projects, employees
  - budget_id = BDG-{project_id}-{wbs_code}-{version}
  - cost_type: LABOUR / MATERIAL / EQUIPMENT / SUBCONTRACT / OVERHEAD / CONTINGENCY
  - budget_version: ORIGINAL 40%, REV1 30%, REV2 20%, REV3 10%
  - contingency = 5-15%
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
OUT_FILE  = os.path.join(OUT_DIR, 'project_budgets.xlsx')
SEED      = 42

fake = Faker()
Faker.seed(SEED)
np.random.seed(SEED)
random.seed(SEED)

os.makedirs(OUT_DIR, exist_ok=True)

# ── Load FK references ────────────────────────────────────────────────────────
projects_df  = pd.read_excel(os.path.join(OUT_DIR, 'projects.xlsx'), sheet_name='projects')
employees_df = pd.read_excel(os.path.join(OUT_DIR, 'employees.xlsx'), sheet_name='employees')

project_ids  = projects_df['project_id'].tolist()
proj_value_map = dict(zip(projects_df['project_id'], projects_df['approved_budget_usd']))

fin_emps = employees_df[
    (employees_df['department'].isin(['FINANCE', 'PROJECTS'])) &
    (employees_df['active'] == True)
]
budget_owner_ids = fin_emps['employee_id'].tolist() if len(fin_emps) > 0 else employees_df['employee_id'].tolist()

# ── Lookup tables ─────────────────────────────────────────────────────────────
COST_TYPES   = ['LABOUR', 'MATERIAL', 'EQUIPMENT', 'SUBCONTRACT', 'OVERHEAD', 'CONTINGENCY']
COST_TYPE_W  = [0.20, 0.30, 0.15, 0.20, 0.10, 0.05]

VERSIONS     = ['ORIGINAL', 'REV1', 'REV2', 'REV3']
VERSION_W    = [0.40, 0.30, 0.20, 0.10]

BUDGET_STATUS = ['DRAFT', 'SUBMITTED', 'APPROVED', 'ACTIVE', 'CLOSED', 'FROZEN']
BUDGET_STATUS_W = [0.05, 0.10, 0.25, 0.40, 0.15, 0.05]

WBS_L1 = ['01', '02', '03', '04', '05', '06', '07', '08']
WBS_L2 = ['01', '02', '03', '04', '05']


# ── Generation ────────────────────────────────────────────────────────────────
def generate_project_budgets(n):
    rows = []

    values = np.random.lognormal(mean=12.0, sigma=1.5, size=n)
    values = np.clip(values, 10_000, 50_000_000)

    for i in range(n):
        project_id = random.choice(project_ids)
        proj_val   = proj_value_map.get(project_id, 1_000_000)

        wbs_code = f"{random.choice(WBS_L1)}.{random.choice(WBS_L2)}"
        version  = random.choices(VERSIONS, VERSION_W)[0]

        budget_id = f"BDG-{project_id}-{wbs_code}-{version}"

        cost_type = random.choices(COST_TYPES, COST_TYPE_W)[0]
        status    = random.choices(BUDGET_STATUS, BUDGET_STATUS_W)[0]

        # Budget amount relative to project value
        base_amount = round(float(values[i]), 2)

        # Contingency 5-15%
        contingency_pct = round(random.uniform(0.05, 0.15), 4)
        contingency_amt = round(base_amount * contingency_pct, 2) if cost_type == 'CONTINGENCY' else 0.0

        # Revised amount for non-ORIGINAL versions
        if version == 'ORIGINAL':
            revised_amount = base_amount
        else:
            factor = random.uniform(0.85, 1.25)
            revised_amount = round(base_amount * factor, 2)

        committed   = round(revised_amount * random.uniform(0.3, 0.9), 2)
        actual_spent = round(committed * random.uniform(0.5, 1.0), 2)
        remaining   = round(revised_amount - actual_spent, 2)

        year = random.randint(2019, 2025)
        period_start = datetime(year, random.randint(1, 12), 1)
        period_end   = period_start + timedelta(days=random.randint(90, 365))

        rows.append({
            'budget_id':        budget_id,
            'project_id':       project_id,
            'wbs_code':         wbs_code,
            'cost_type':        cost_type,
            'budget_version':   version,
            'budget_status':    status,
            'original_amount':  base_amount,
            'revised_amount':   revised_amount,
            'contingency_pct':  contingency_pct,
            'contingency_amount': contingency_amt,
            'committed_amount': committed,
            'actual_spent':     actual_spent,
            'remaining_budget': remaining,
            'currency':         random.choices(['SAR', 'USD', 'EUR'], [0.60, 0.30, 0.10])[0],
            'period_start':     period_start.strftime('%Y-%m-%d'),
            'period_end':       period_end.strftime('%Y-%m-%d'),
            'budget_owner_id':  random.choice(budget_owner_ids),
            'approval_date':    (period_start - timedelta(days=random.randint(7, 30))).strftime('%Y-%m-%d') if status not in ('DRAFT', 'SUBMITTED') else None,
            'notes':            fake.sentence(nb_words=5) if random.random() < 0.10 else None,
            'created_date':     (period_start - timedelta(days=random.randint(30, 60))).strftime('%Y-%m-%d'),
        })

    return pd.DataFrame(rows)


# ── Write to Excel ────────────────────────────────────────────────────────────
def main():
    print(f"  Generating {N:,} project budget records...", flush=True)
    df = generate_project_budgets(N)

    print(f"  Writing to {OUT_FILE}...", flush=True)
    with pd.ExcelWriter(OUT_FILE, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='project_budgets')
        meta = pd.DataFrame({
            'key':   ['source', 'records', 'generated_at', 'seed', 'schema_version'],
            'value': ['project_budgets.xlsx', str(len(df)),
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
    assert df['project_id'].isin(project_ids).all()
    # Contingency pct 5-15%
    assert (df['contingency_pct'] >= 0.05).all() and (df['contingency_pct'] <= 0.15).all()
    # Version distribution roughly matches
    ver_counts = df['budget_version'].value_counts(normalize=True)
    assert ver_counts.get('ORIGINAL', 0) > 0.30, "ORIGINAL share too low"
    print(f"  Validation: PASSED")


if __name__ == '__main__':
    main()