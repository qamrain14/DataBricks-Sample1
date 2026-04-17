# Databricks notebook source
"""
Generator 03: Material Master
===============================
Output : data/raw/materials.xlsx
Records: 100,000
Seed   : 42
Domain : Master Data — Materials

Business Rules:
  - Self-referencing hierarchy: PRODUCT (top) → ITEM → COMPONENT → ASSEMBLY
  - PRODUCT level has parent_material_id = NULL
  - 15 product categories with realistic UOMs and prices
  - Hazardous for CHEMICALS/FUEL categories (25%)
  - Requires inspection 40%, strategic item 15%, local content 40%
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
OUT_FILE  = os.path.join(OUT_DIR, 'materials.xlsx')
SEED      = 42

fake = Faker(['en_US', 'en_GB'])
Faker.seed(SEED)
np.random.seed(SEED)
random.seed(SEED)

os.makedirs(OUT_DIR, exist_ok=True)

# ── Lookup tables ─────────────────────────────────────────────────────────────
MATERIAL_LEVELS = ['PRODUCT', 'ITEM', 'COMPONENT', 'ASSEMBLY']
LEVEL_W         = [0.20, 0.60, 0.15, 0.05]

CATEGORIES = [
    'STRUCTURAL_STEEL', 'CONCRETE', 'PIPES', 'VALVES', 'ELECTRICAL',
    'INSTRUMENTS', 'CIVIL', 'MECHANICAL', 'CHEMICALS', 'SAFETY',
    'IT', 'SERVICES', 'EQUIPMENT', 'FUEL', 'CONSUMABLES',
]
CATEGORY_W = [0.12, 0.10, 0.10, 0.08, 0.08, 0.07, 0.07, 0.07, 0.05,
              0.05, 0.04, 0.04, 0.05, 0.04, 0.04]

CAT_CODES = {
    'STRUCTURAL_STEEL': 'STL', 'CONCRETE': 'CON', 'PIPES': 'PIP', 'VALVES': 'VLV',
    'ELECTRICAL': 'ELE', 'INSTRUMENTS': 'INS', 'CIVIL': 'CIV', 'MECHANICAL': 'MEC',
    'CHEMICALS': 'CHM', 'SAFETY': 'SAF', 'IT': 'ITQ', 'SERVICES': 'SVC',
    'EQUIPMENT': 'EQP', 'FUEL': 'FUE', 'CONSUMABLES': 'CSM',
}

SUB_CATEGORIES = {
    'STRUCTURAL_STEEL': ['Beams', 'Plates', 'Angles', 'Channels', 'Rebar'],
    'CONCRETE': ['Ready-Mix', 'Precast', 'Grout', 'Admixtures', 'Cement'],
    'PIPES': ['Carbon Steel', 'Stainless Steel', 'GRP', 'HDPE', 'Copper'],
    'VALVES': ['Gate', 'Ball', 'Check', 'Butterfly', 'Relief', 'Globe'],
    'ELECTRICAL': ['Cables', 'Switchgear', 'Transformers', 'Panels', 'MCBs'],
    'INSTRUMENTS': ['Transmitters', 'Flow Meters', 'Level Gauges', 'Thermocouples', 'Analyzers'],
    'CIVIL': ['Sand', 'Gravel', 'Fill Material', 'Geotextile', 'Asphalt'],
    'MECHANICAL': ['Pumps', 'Compressors', 'Heat Exchangers', 'Tanks', 'Fans'],
    'CHEMICALS': ['Corrosion Inhibitor', 'Demulsifier', 'Biocide', 'Solvent', 'Catalyst'],
    'SAFETY': ['PPE', 'Fire Extinguisher', 'Safety Signage', 'First Aid Kit', 'Gas Detector'],
    'IT': ['Laptop', 'Monitor', 'Switch', 'Router', 'UPS'],
    'SERVICES': ['Labour Hour', 'Crane Rental', 'Transport', 'Scaffolding', 'Inspection'],
    'EQUIPMENT': ['Crane', 'Excavator', 'Generator', 'Forklift', 'Welder'],
    'FUEL': ['Diesel', 'Gasoline', 'LPG', 'Aviation Fuel', 'Natural Gas'],
    'CONSUMABLES': ['Welding Rod', 'Grinding Disc', 'Sealant', 'Bolt', 'Gasket'],
}

UOMS = {
    'STRUCTURAL_STEEL': ['KG', 'MT'],
    'CONCRETE': ['CUM', 'BAG'],
    'PIPES': ['RM', 'JOINT'],
    'VALVES': ['NOS', 'SET'],
    'ELECTRICAL': ['RM', 'NOS', 'SET'],
    'INSTRUMENTS': ['NOS', 'SET'],
    'CIVIL': ['CUM', 'MT', 'SQM'],
    'MECHANICAL': ['NOS', 'SET'],
    'CHEMICALS': ['LTR', 'KG', 'DRUM'],
    'SAFETY': ['NOS', 'SET', 'BOX'],
    'IT': ['NOS', 'SET'],
    'SERVICES': ['HR', 'DAY', 'MONTH'],
    'EQUIPMENT': ['NOS', 'DAY', 'MONTH'],
    'FUEL': ['LTR', 'GAL', 'BBL'],
    'CONSUMABLES': ['NOS', 'KG', 'BOX', 'PKT'],
}

PRICE_RANGES = {
    'STRUCTURAL_STEEL': (500, 3000), 'CONCRETE': (80, 500), 'PIPES': (20, 2000),
    'VALVES': (100, 50000), 'ELECTRICAL': (10, 100000), 'INSTRUMENTS': (200, 80000),
    'CIVIL': (5, 200), 'MECHANICAL': (1000, 500000), 'CHEMICALS': (50, 5000),
    'SAFETY': (5, 2000), 'IT': (100, 5000), 'SERVICES': (50, 5000),
    'EQUIPMENT': (500, 500000), 'FUEL': (1, 200), 'CONSUMABLES': (1, 500),
}

LEAD_TIMES = {
    'STRUCTURAL_STEEL': 90, 'CONCRETE': 7, 'PIPES': 60, 'VALVES': 120,
    'ELECTRICAL': 45, 'INSTRUMENTS': 90, 'CIVIL': 14, 'MECHANICAL': 120,
    'CHEMICALS': 30, 'SAFETY': 14, 'IT': 21, 'SERVICES': 7,
    'EQUIPMENT': 180, 'FUEL': 7, 'CONSUMABLES': 14,
}

HAZARDOUS_CATEGORIES = {'CHEMICALS', 'FUEL'}

# Fake UNSPSC codes
UNSPSC_BASE = {
    'STRUCTURAL_STEEL': '30100000', 'CONCRETE': '30110000', 'PIPES': '40140000',
    'VALVES': '40140100', 'ELECTRICAL': '26100000', 'INSTRUMENTS': '41110000',
    'CIVIL': '30100000', 'MECHANICAL': '40170000', 'CHEMICALS': '12140000',
    'SAFETY': '46180000', 'IT': '43210000', 'SERVICES': '72100000',
    'EQUIPMENT': '22100000', 'FUEL': '15100000', 'CONSUMABLES': '31160000',
}


# ── Generation ────────────────────────────────────────────────────────────────
def generate_materials(n):
    # First pass: create all product-level materials to use as parents
    product_ids = []
    rows = []

    for i in range(n):
        level   = random.choices(MATERIAL_LEVELS, LEVEL_W)[0]
        cat     = random.choices(CATEGORIES, CATEGORY_W)[0]
        cc      = CAT_CODES[cat]
        sub     = random.choice(SUB_CATEGORIES[cat])

        mat_id  = f"MAT-{i+1:08d}"
        mat_code = f"{cc}-{sub[:3].upper()}-{i+1:05d}"

        # Parent reference
        parent_id = None
        if level == 'PRODUCT':
            product_ids.append(mat_id)
        elif product_ids:
            parent_id = random.choice(product_ids)

        uom = random.choice(UOMS[cat])
        lo, hi = PRICE_RANGES[cat]
        unit_price = round(random.uniform(lo, hi), 2)
        currency = random.choices(['USD', 'EUR', 'GBP', 'SAR'], [0.60, 0.20, 0.10, 0.10])[0]

        base_lead = LEAD_TIMES[cat]
        lead_time_days = random.randint(max(1, base_lead - base_lead // 3),
                                        base_lead + base_lead // 3)

        hazardous = cat in HAZARDOUS_CATEGORIES and random.random() < 0.25
        requires_inspection = random.random() < 0.40
        strategic_item = random.random() < 0.15
        local_content = random.random() < 0.40

        weight_kg = round(random.uniform(0.01, 5000.0), 2) if cat in {
            'STRUCTURAL_STEEL', 'CONCRETE', 'PIPES', 'MECHANICAL', 'EQUIPMENT'} else None

        unspsc = UNSPSC_BASE.get(cat, '00000000')
        unspsc_detail = f"{unspsc[:6]}{random.randint(10, 99)}"

        min_stock     = random.randint(10, 500)
        max_stock     = min_stock * random.randint(3, 10)
        reorder_point = random.randint(min_stock, min_stock * 2)
        reorder_qty   = random.randint(min_stock, max_stock)

        created = (datetime(2018, 1, 1) + timedelta(days=random.randint(0, 2190))).strftime('%Y-%m-%d')

        rows.append({
            'material_id':         mat_id,
            'material_code':       mat_code,
            'material_name':       f"{sub} - {fake.word().capitalize()} {random.randint(100,999)}",
            'material_level':      level,
            'parent_material_id':  parent_id,
            'category':            cat,
            'sub_category':        sub,
            'unit_of_measure':     uom,
            'unit_price':          unit_price,
            'currency':            currency,
            'lead_time_days':      lead_time_days,
            'hazardous':           hazardous,
            'requires_inspection': requires_inspection,
            'strategic_item':      strategic_item,
            'local_content':       local_content,
            'weight_kg':           weight_kg,
            'unspsc_code':         unspsc_detail,
            'min_stock_level':     min_stock,
            'max_stock_level':     max_stock,
            'reorder_point':       reorder_point,
            'reorder_quantity':    reorder_qty,
            'shelf_life_days':     random.randint(30, 3650) if cat in {'CHEMICALS', 'FUEL', 'CONSUMABLES'} else None,
            'manufacturer':        fake.company(),
            'brand':               fake.company_suffix() + ' ' + fake.word().capitalize(),
            'country_of_origin':   random.choice(['China', 'USA', 'Germany', 'India', 'Japan', 'UAE', 'UK', 'Italy', 'South Korea', 'France']),
            'active':              random.random() < 0.95,
            'created_date':        created,
        })

    return pd.DataFrame(rows)


# ── Write to Excel ────────────────────────────────────────────────────────────
def main():
    print(f"  Generating {N:,} material records...", flush=True)
    df = generate_materials(N)

    print(f"  Writing to {OUT_FILE}...", flush=True)
    with pd.ExcelWriter(OUT_FILE, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='materials')
        meta = pd.DataFrame({
            'key':   ['source', 'records', 'generated_at', 'seed', 'schema_version'],
            'value': ['materials.xlsx', str(len(df)),
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
    assert df['material_id'].nunique() == N, "Duplicate material_ids"
    assert df['category'].isin(CATEGORIES).all()
    assert df['material_level'].isin(MATERIAL_LEVELS).all()
    # PRODUCT level should have no parent
    assert df[df['material_level'] == 'PRODUCT']['parent_material_id'].isna().all()
    print(f"  Validation: PASSED")


if __name__ == '__main__':
    main()