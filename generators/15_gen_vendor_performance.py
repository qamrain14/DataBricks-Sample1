# Databricks notebook source
"""
Generator 15: Vendor Performance Scorecards
=============================================
Output : data/raw/vendor_performance.xlsx
Records: 100,000
Seed   : 42
Domain : Analytical — Procurement

Business Rules:
  - FK → vendors, projects, employees (PROCUREMENT / PROJECTS dept)
  - performance_id = PERF-{vendor_id}-{YYYYMM}
  - Weighted composite: delivery(30%) + quality(30%) + commercial(20%) + hse(20%)
  - Recommendation: PREFERRED / APPROVED / CONDITIONAL / DELIST
"""

import os
import random
import numpy as np
import pandas as pd
from faker import Faker
from datetime import datetime

# ── Configuration ────────────────────────────────────────────────────────────
N         = int(os.getenv('N_RECORDS', 100_000))
OUT_DIR   = os.getenv('OUTPUT_DIR', os.path.join(os.path.dirname(__file__), '..', 'data', 'raw'))
OUT_FILE  = os.path.join(OUT_DIR, 'vendor_performance.xlsx')
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

vendor_ids  = vendors_df['vendor_id'].tolist()
project_ids = projects_df['project_id'].tolist()

# Evaluators from PROCUREMENT and PROJECTS departments
eval_df = employees_df[employees_df['department'].isin(['PROCUREMENT', 'PROJECTS'])]
if eval_df.empty:
    eval_df = employees_df
evaluator_ids = eval_df['employee_id'].tolist()

# ── Lookup tables ─────────────────────────────────────────────────────────────
EVAL_PERIODS = [f"{y}{m:02d}" for y in range(2019, 2026) for m in range(1, 13)]

DELIVERY_METRICS = [
    'ON_TIME_DELIVERY_RATE', 'LEAD_TIME_VARIANCE', 'DELIVERY_COMPLETENESS',
    'DOCUMENTATION_ACCURACY', 'SCHEDULE_ADHERENCE',
]
QUALITY_METRICS = [
    'DEFECT_RATE', 'FIRST_PASS_YIELD', 'NCR_COUNT', 'REWORK_RATE',
    'SPECIFICATION_COMPLIANCE',
]
COMMERCIAL_METRICS = [
    'PRICE_COMPETITIVENESS', 'INVOICE_ACCURACY', 'PAYMENT_TERMS_COMPLIANCE',
    'CHANGE_ORDER_FREQUENCY',
]
HSE_METRICS = [
    'INCIDENT_RATE', 'NEAR_MISS_REPORTING', 'HSE_COMPLIANCE', 'SAFETY_AUDIT_SCORE',
]

# Weights for composite score
W_DELIVERY   = 0.30
W_QUALITY    = 0.30
W_COMMERCIAL = 0.20
W_HSE        = 0.20

# Recommendation thresholds (composite_score out of 100)
# >=80 PREFERRED, >=60 APPROVED, >=40 CONDITIONAL, <40 DELIST


# ── Generation ────────────────────────────────────────────────────────────────
def _clamp(val, lo=0.0, hi=100.0):
    return max(lo, min(hi, val))


def generate_vendor_performance(n):
    rows = []

    for i in range(n):
        vendor_id = random.choice(vendor_ids)
        period    = random.choice(EVAL_PERIODS)
        perf_id   = f"PERF-{vendor_id}-{period}"

        project_id   = random.choice(project_ids)
        evaluator_id = random.choice(evaluator_ids)

        # Raw sub-scores (0-100, normally distributed around ~70)
        delivery_score   = _clamp(round(np.random.normal(72, 15), 2))
        quality_score    = _clamp(round(np.random.normal(70, 16), 2))
        commercial_score = _clamp(round(np.random.normal(68, 14), 2))
        hse_score        = _clamp(round(np.random.normal(75, 13), 2))

        composite_score = round(
            delivery_score   * W_DELIVERY +
            quality_score    * W_QUALITY  +
            commercial_score * W_COMMERCIAL +
            hse_score        * W_HSE,
            2,
        )

        if composite_score >= 80:
            recommendation = 'PREFERRED'
        elif composite_score >= 60:
            recommendation = 'APPROVED'
        elif composite_score >= 40:
            recommendation = 'CONDITIONAL'
        else:
            recommendation = 'DELIST'

        # Metric highlights (random selection per dimension)
        delivery_metric   = random.choice(DELIVERY_METRICS)
        quality_metric    = random.choice(QUALITY_METRICS)
        commercial_metric = random.choice(COMMERCIAL_METRICS)
        hse_metric        = random.choice(HSE_METRICS)

        # Number of POs evaluated in this period
        po_count_evaluated = random.randint(1, 25)
        total_value_evaluated = round(np.random.lognormal(mean=11, sigma=1.2), 2)

        eval_date = f"{period[:4]}-{period[4:]}-{random.randint(1, 28):02d}"

        corrective_actions = random.randint(0, 3) if composite_score < 60 else 0
        improvement_trend  = random.choice(['IMPROVING', 'STABLE', 'DECLINING'])

        rows.append({
            'performance_id':         perf_id,
            'vendor_id':              vendor_id,
            'project_id':             project_id,
            'evaluation_period':      period,
            'evaluator_id':           evaluator_id,
            'delivery_score':         delivery_score,
            'quality_score':          quality_score,
            'commercial_score':       commercial_score,
            'hse_score':              hse_score,
            'composite_score':        composite_score,
            'recommendation':         recommendation,
            'delivery_metric_focus':  delivery_metric,
            'quality_metric_focus':   quality_metric,
            'commercial_metric_focus':commercial_metric,
            'hse_metric_focus':       hse_metric,
            'po_count_evaluated':     po_count_evaluated,
            'total_value_evaluated':  total_value_evaluated,
            'corrective_actions':     corrective_actions,
            'improvement_trend':      improvement_trend,
            'evaluation_date':        eval_date,
            'remarks':                fake.sentence(nb_words=5) if random.random() < 0.10 else None,
            'created_date':           eval_date,
        })

    return pd.DataFrame(rows)


# ── Write to Excel ────────────────────────────────────────────────────────────
def main():
    print(f"  Generating {N:,} vendor performance records...", flush=True)
    df = generate_vendor_performance(N)

    print(f"  Writing to {OUT_FILE}...", flush=True)
    with pd.ExcelWriter(OUT_FILE, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='vendor_performance')
        meta = pd.DataFrame({
            'key':   ['source', 'records', 'generated_at', 'seed', 'schema_version'],
            'value': ['vendor_performance.xlsx', str(len(df)),
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
    assert df['vendor_id'].isin(vendor_ids).all()
    assert df['project_id'].isin(project_ids).all()
    assert df['evaluator_id'].isin(evaluator_ids).all()
    # Composite score = weighted sum
    recalc = (
        df['delivery_score']   * W_DELIVERY +
        df['quality_score']    * W_QUALITY  +
        df['commercial_score'] * W_COMMERCIAL +
        df['hse_score']        * W_HSE
    ).round(2)
    assert np.allclose(df['composite_score'], recalc, atol=0.01), "Composite score mismatch"
    # Recommendation thresholds
    pref = df[df['recommendation'] == 'PREFERRED']
    assert (pref['composite_score'] >= 80).all(), "PREFERRED should have score >= 80"
    delist = df[df['recommendation'] == 'DELIST']
    assert (delist['composite_score'] < 40).all(), "DELIST should have score < 40"
    print(f"  Validation: PASSED")


if __name__ == '__main__':
    main()