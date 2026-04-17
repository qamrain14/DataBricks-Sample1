# Databricks notebook source
"""
Generator 04: Employee Master
===============================
Output : data/raw/employees.xlsx
Records: 100,000
Seed   : 42
Domain : Master Data — Employees

Business Rules:
  - 7 departments: PROCUREMENT, ENGINEERING, FINANCE, PROJECTS, HSE, LEGAL, CONTRACTS
  - Grades L1-L6 with approval limits (L1=10K → L6=unlimited)
  - Active 90%, hire dates 2010-2023
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
OUT_FILE  = os.path.join(OUT_DIR, 'employees.xlsx')
SEED      = 42

fake = Faker(['en_US', 'en_GB', 'ar_SA'])
Faker.seed(SEED)
np.random.seed(SEED)
random.seed(SEED)

os.makedirs(OUT_DIR, exist_ok=True)

# ── Lookup tables ─────────────────────────────────────────────────────────────
DEPARTMENTS = ['PROCUREMENT', 'ENGINEERING', 'FINANCE', 'PROJECTS', 'HSE', 'LEGAL', 'CONTRACTS']
DEPT_W      = [0.25, 0.20, 0.10, 0.20, 0.10, 0.05, 0.10]
DEPT_CODES  = {
    'PROCUREMENT': 'PRO', 'ENGINEERING': 'ENG', 'FINANCE': 'FIN',
    'PROJECTS': 'PRJ', 'HSE': 'HSE', 'LEGAL': 'LEG', 'CONTRACTS': 'CTR',
}

GRADES = ['L1', 'L2', 'L3', 'L4', 'L5', 'L6']
GRADE_W = [0.30, 0.25, 0.20, 0.12, 0.08, 0.05]
APPROVAL_LIMITS = {
    'L1': 10_000, 'L2': 50_000, 'L3': 200_000,
    'L4': 1_000_000, 'L5': 10_000_000, 'L6': None,  # unlimited
}

JOB_TITLES = {
    'PROCUREMENT': ['Buyer', 'Senior Buyer', 'Procurement Specialist', 'Category Manager', 'Procurement Lead', 'Procurement Manager'],
    'ENGINEERING': ['Engineer', 'Senior Engineer', 'Lead Engineer', 'Design Engineer', 'Project Engineer', 'Engineering Manager'],
    'FINANCE': ['Accountant', 'Financial Analyst', 'AP Specialist', 'Cost Controller', 'Finance Manager', 'CFO'],
    'PROJECTS': ['Project Coordinator', 'Planner', 'Scheduler', 'Project Manager', 'PMO Lead', 'Program Director'],
    'HSE': ['HSE Officer', 'Safety Inspector', 'Environmental Specialist', 'HSE Manager', 'HSE Director', 'Chief Safety Officer'],
    'LEGAL': ['Legal Officer', 'Contract Analyst', 'Legal Counsel', 'Senior Counsel', 'Legal Manager', 'General Counsel'],
    'CONTRACTS': ['Contract Admin', 'Contract Specialist', 'Contract Engineer', 'Claims Analyst', 'Contracts Manager', 'Contracts Director'],
}

LOCATIONS = ['Head Office', 'Site Office A', 'Site Office B', 'Warehouse', 'Regional Office', 'Field Camp']
LOCATION_W = [0.30, 0.20, 0.15, 0.10, 0.15, 0.10]


# ── Generation ────────────────────────────────────────────────────────────────
def generate_employees(n):
    rows = []
    hire_start = datetime(2010, 1, 1)
    hire_end   = datetime(2023, 12, 31)
    hire_range = (hire_end - hire_start).days

    for i in range(n):
        dept    = random.choices(DEPARTMENTS, DEPT_W)[0]
        dc      = DEPT_CODES[dept]
        grade   = random.choices(GRADES, GRADE_W)[0]
        grade_idx = GRADES.index(grade)

        emp_id   = f"EMP-{i+1:06d}"
        emp_code = f"{dc}{i+1:05d}"

        title_list = JOB_TITLES[dept]
        # Higher grade → higher title index
        title_idx  = min(grade_idx, len(title_list) - 1)
        job_title  = title_list[title_idx]

        hire_date = (hire_start + timedelta(days=random.randint(0, hire_range))).strftime('%Y-%m-%d')
        active    = random.random() < 0.90

        location = random.choices(LOCATIONS, LOCATION_W)[0]

        # Reporting: L1 reports to random L2-L6, etc.
        # We'll assign manager_id later or leave as pattern
        reporting_grade = GRADES[min(grade_idx + 1, 5)]

        approval_limit = APPROVAL_LIMITS[grade]

        email = f"{emp_code.lower()}@company.com"
        phone = fake.phone_number()

        certification = None
        if dept == 'HSE' and random.random() < 0.60:
            certification = random.choice(['NEBOSH', 'OSHA 30', 'IOSH', 'CSP', 'ISO 45001 Lead Auditor'])
        elif dept == 'PROCUREMENT' and random.random() < 0.40:
            certification = random.choice(['CIPS', 'CSCP', 'CPM', 'CPSM'])
        elif dept == 'ENGINEERING' and random.random() < 0.30:
            certification = random.choice(['PE', 'PMP', 'CEng', 'LEED AP'])
        elif dept == 'PROJECTS' and random.random() < 0.50:
            certification = random.choice(['PMP', 'PRINCE2', 'PMI-ACP', 'PMI-SP'])
        elif dept == 'FINANCE' and random.random() < 0.35:
            certification = random.choice(['CPA', 'CMA', 'ACCA', 'CFA'])

        rows.append({
            'employee_id':     emp_id,
            'employee_code':   emp_code,
            'first_name':      fake.first_name(),
            'last_name':       fake.last_name(),
            'email':           email,
            'phone':           phone,
            'department':      dept,
            'job_title':       job_title,
            'grade':           grade,
            'approval_limit':  approval_limit,
            'hire_date':       hire_date,
            'active':          active,
            'location':        location,
            'certification':   certification,
            'nationality':     random.choice(['Saudi', 'Indian', 'Pakistani', 'Egyptian', 'Filipino',
                                              'British', 'American', 'Jordanian', 'Sudanese', 'Bangladeshi']),
            'years_experience': random.randint(1, 35),
            'cost_center':     f"CC-{dc}-{random.randint(100, 999)}",
            'created_date':    hire_date,
        })

    return pd.DataFrame(rows)


# ── Write to Excel ────────────────────────────────────────────────────────────
def main():
    print(f"  Generating {N:,} employee records...", flush=True)
    df = generate_employees(N)

    print(f"  Writing to {OUT_FILE}...", flush=True)
    with pd.ExcelWriter(OUT_FILE, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='employees')
        meta = pd.DataFrame({
            'key':   ['source', 'records', 'generated_at', 'seed', 'schema_version'],
            'value': ['employees.xlsx', str(len(df)),
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
    assert df['employee_id'].nunique() == N, "Duplicate employee_ids"
    assert df['department'].isin(DEPARTMENTS).all()
    assert df['grade'].isin(GRADES).all()
    # L6 should have None approval limit
    l6_mask = df['grade'] == 'L6'
    assert df[l6_mask]['approval_limit'].isna().all(), "L6 should have unlimited (None) approval"
    print(f"  Validation: PASSED")


if __name__ == '__main__':
    main()