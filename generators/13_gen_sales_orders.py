# Databricks notebook source
"""
Generator 13: Sales Orders
=============================
Output : data/raw/sales_orders.xlsx
Records: 100,000
Seed   : 42
Domain : Transactional — Revenue

Business Rules:
  - FK → projects, materials, employees
  - sales_order_id = SO-{YEAR}-{7digit}
  - cogs = 60-85% of revenue
  - payment_status: UNBILLED / BILLED / PARTIALLY_PAID / PAID / OVERDUE
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
OUT_FILE  = os.path.join(OUT_DIR, 'sales_orders.xlsx')
SEED      = 42

fake = Faker()
Faker.seed(SEED)
np.random.seed(SEED)
random.seed(SEED)

os.makedirs(OUT_DIR, exist_ok=True)

# ── Load FK references ────────────────────────────────────────────────────────
projects_df  = pd.read_excel(os.path.join(OUT_DIR, 'projects.xlsx'), sheet_name='projects')
materials_df = pd.read_excel(os.path.join(OUT_DIR, 'materials.xlsx'), sheet_name='materials')
employees_df = pd.read_excel(os.path.join(OUT_DIR, 'employees.xlsx'), sheet_name='employees')

project_ids  = projects_df['project_id'].tolist()
material_ids = materials_df['material_id'].tolist()
mat_price_map = dict(zip(materials_df['material_id'], materials_df['unit_price']))

sales_emps = employees_df[
    (employees_df['department'].isin(['PROCUREMENT', 'PROJECTS'])) &
    (employees_df['active'] == True)
]
sales_rep_ids = sales_emps['employee_id'].tolist() if len(sales_emps) > 0 else employees_df['employee_id'].tolist()

# ── Lookup tables ─────────────────────────────────────────────────────────────
ORDER_TYPE    = ['STANDARD', 'BLANKET', 'RETURN', 'CREDIT_NOTE']
ORDER_TYPE_W  = [0.65, 0.15, 0.10, 0.10]

ORDER_STATUS  = ['DRAFT', 'CONFIRMED', 'IN_PROGRESS', 'DELIVERED', 'CLOSED', 'CANCELLED']
ORDER_STATUS_W = [0.05, 0.15, 0.20, 0.25, 0.25, 0.10]

PAYMENT_STATUS = ['UNBILLED', 'BILLED', 'PARTIALLY_PAID', 'PAID', 'OVERDUE']
PAYMENT_STATUS_W = [0.10, 0.20, 0.15, 0.40, 0.15]

CUSTOMER_TYPES = ['INTERNAL', 'EXTERNAL', 'GOVERNMENT', 'JV_PARTNER']
CUSTOMER_TYPE_W = [0.30, 0.40, 0.20, 0.10]

DELIVERY_TERMS = ['EXW', 'FOB', 'CIF', 'DDP', 'DAP', 'FCA']


# ── Generation ────────────────────────────────────────────────────────────────
def generate_sales_orders(n):
    rows = []

    revenues = np.random.lognormal(mean=11.0, sigma=1.5, size=n)
    revenues = np.clip(revenues, 5_000, 25_000_000)

    for i in range(n):
        year = random.randint(2019, 2025)
        so_id = f"SO-{year}-{i+1:07d}"

        project_id = random.choice(project_ids)
        mat_id     = random.choice(material_ids)
        sales_rep  = random.choice(sales_rep_ids)

        order_type = random.choices(ORDER_TYPE, ORDER_TYPE_W)[0]
        status     = random.choices(ORDER_STATUS, ORDER_STATUS_W)[0]

        if status == 'CANCELLED':
            payment_status = 'UNBILLED'
        else:
            payment_status = random.choices(PAYMENT_STATUS, PAYMENT_STATUS_W)[0]

        revenue = round(float(revenues[i]), 2)

        # COGS = 60-85% of revenue
        cogs_pct = random.uniform(0.60, 0.85)
        cogs     = round(revenue * cogs_pct, 2)
        margin   = round(revenue - cogs, 2)
        margin_pct = round((margin / revenue) * 100, 2) if revenue > 0 else 0

        quantity   = random.randint(1, 500)
        unit_price = round(revenue / quantity, 2) if quantity > 0 else revenue

        tax_rate   = random.choice([0.0, 0.05, 0.15])
        tax_amount = round(revenue * tax_rate, 2)
        total      = round(revenue + tax_amount, 2)

        order_date = datetime(year, 1, 1) + timedelta(days=random.randint(0, 364))
        delivery_date = order_date + timedelta(days=random.randint(14, 180))

        customer_type = random.choices(CUSTOMER_TYPES, CUSTOMER_TYPE_W)[0]
        customer_name = fake.company()

        if order_type == 'RETURN':
            revenue = -abs(revenue)
            total   = -abs(total)
        elif order_type == 'CREDIT_NOTE':
            revenue = -abs(revenue)
            cogs    = -abs(cogs)
            total   = -abs(total)

        rows.append({
            'sales_order_id':   so_id,
            'project_id':       project_id,
            'material_id':      mat_id,
            'order_type':       order_type,
            'order_status':     status,
            'payment_status':   payment_status,
            'customer_name':    customer_name,
            'customer_type':    customer_type,
            'sales_rep_id':     sales_rep,
            'order_date':       order_date.strftime('%Y-%m-%d'),
            'delivery_date':    delivery_date.strftime('%Y-%m-%d'),
            'quantity':         quantity,
            'unit_price':       unit_price,
            'revenue':          revenue,
            'cogs':             cogs,
            'gross_margin':     margin,
            'margin_pct':       margin_pct,
            'tax_rate':         tax_rate,
            'tax_amount':       tax_amount,
            'total_amount':     total,
            'currency':         random.choices(['SAR', 'USD', 'EUR'], [0.55, 0.30, 0.15])[0],
            'delivery_terms':   random.choice(DELIVERY_TERMS),
            'description':      fake.sentence(nb_words=5),
            'created_date':     order_date.strftime('%Y-%m-%d'),
        })

    return pd.DataFrame(rows)


# ── Write to Excel ────────────────────────────────────────────────────────────
def main():
    print(f"  Generating {N:,} sales order records...", flush=True)
    df = generate_sales_orders(N)

    print(f"  Writing to {OUT_FILE}...", flush=True)
    with pd.ExcelWriter(OUT_FILE, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='sales_orders')
        meta = pd.DataFrame({
            'key':   ['source', 'records', 'generated_at', 'seed', 'schema_version'],
            'value': ['sales_orders.xlsx', str(len(df)),
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
    assert df['sales_order_id'].nunique() == N
    assert df['project_id'].isin(project_ids).all()
    assert df['material_id'].isin(material_ids).all()
    # CANCELLED → UNBILLED
    cancelled = df[df['order_status'] == 'CANCELLED']
    assert (cancelled['payment_status'] == 'UNBILLED').all(), "CANCELLED should be UNBILLED"
    print(f"  Validation: PASSED")


if __name__ == '__main__':
    main()