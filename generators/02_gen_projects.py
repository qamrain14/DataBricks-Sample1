# Databricks notebook source
"""
Generator 02: Project Master
==============================
Output : data/raw/projects.xlsx
Records: 100,000
Seed   : 42
Domain : Master Data — Projects

Business Rules:
  - HIGHWAY and BRIDGE → CIVIL_CONSTRUCTION sector
  - REFINERY, PIPELINE, DRILLING, LNG → OIL_GAS
  - POWER_THERMAL, POWER_SOLAR, POWER_WIND, SUBSTATION → POWER
  - PORT, WATER_TREATMENT → INFRASTRUCTURE
  - Contract values: lognormal per type, CRITICAL priority → top tier
  - Environmental clearance 85% True
"""

import os
import sys
import random
import numpy as np
import pandas as pd
from faker import Faker
from datetime import datetime, timedelta

# ── Configuration ────────────────────────────────────────────────────────────
N         = int(os.getenv('N_RECORDS', 100_000))
OUT_DIR   = os.getenv('OUTPUT_DIR', os.path.join(os.path.dirname(__file__), '..', 'data', 'raw'))
OUT_FILE  = os.path.join(OUT_DIR, 'projects.xlsx')
SEED      = 42

fake = Faker(['en_US', 'en_GB'])
Faker.seed(SEED)
np.random.seed(SEED)
random.seed(SEED)

os.makedirs(OUT_DIR, exist_ok=True)

# ── Lookup tables ─────────────────────────────────────────────────────────────
PROJECT_TYPE_SECTOR = {
    'BUILDING':        'CIVIL_CONSTRUCTION',
    'HIGHWAY':         'CIVIL_CONSTRUCTION',
    'APARTMENT':       'CIVIL_CONSTRUCTION',
    'BRIDGE':          'CIVIL_CONSTRUCTION',
    'REFINERY':        'OIL_GAS',
    'PIPELINE':        'OIL_GAS',
    'DRILLING':        'OIL_GAS',
    'LNG':             'OIL_GAS',
    'POWER_THERMAL':   'POWER',
    'POWER_SOLAR':     'POWER',
    'POWER_WIND':      'POWER',
    'SUBSTATION':      'POWER',
    'PORT':            'INFRASTRUCTURE',
    'WATER_TREATMENT': 'INFRASTRUCTURE',
}

PROJECT_TYPES = list(PROJECT_TYPE_SECTOR.keys())
PROJECT_TYPE_W = [0.12, 0.10, 0.10, 0.05,
                  0.10, 0.08, 0.07, 0.05,
                  0.06, 0.06, 0.05, 0.04,
                  0.06, 0.06]

SUB_SECTORS = {
    'CIVIL_CONSTRUCTION': ['Residential', 'Commercial', 'Industrial', 'Transportation', 'Public Works'],
    'OIL_GAS':            ['Upstream', 'Midstream', 'Downstream', 'Petrochemical', 'LNG Processing'],
    'POWER':              ['Thermal Generation', 'Renewable Energy', 'Transmission', 'Distribution'],
    'INFRASTRUCTURE':     ['Marine', 'Water & Wastewater', 'Urban Development', 'Logistics'],
}

STATUS_LIST = ['BIDDING', 'AWARDED', 'MOBILIZATION', 'CONSTRUCTION', 'COMMISSIONING', 'HANDOVER', 'CLOSED']
STATUS_W    = [0.05, 0.05, 0.10, 0.50, 0.15, 0.10, 0.05]

PHASE_MAP = {
    'BIDDING': 'FEED', 'AWARDED': 'DETAILED_DESIGN', 'MOBILIZATION': 'PROCUREMENT',
    'CONSTRUCTION': 'CONSTRUCTION', 'COMMISSIONING': 'COMMISSIONING',
    'HANDOVER': 'COMMISSIONING', 'CLOSED': 'COMMISSIONING',
}

PRIORITY_LIST = ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW']
PRIORITY_W    = [0.10, 0.30, 0.45, 0.15]

RISK_LIST = ['LOW', 'MEDIUM', 'HIGH', 'EXTREME']
RISK_W    = [0.30, 0.45, 0.20, 0.05]

COUNTRIES = {
    'UAE': 0.20, 'Saudi Arabia': 0.18, 'USA': 0.14, 'UK': 0.09, 'India': 0.08,
    'Australia': 0.05, 'Qatar': 0.05, 'Kuwait': 0.04, 'Oman': 0.04,
    'Canada': 0.03, 'Germany': 0.03, 'France': 0.02, 'Italy': 0.02,
    'South Korea': 0.01, 'Japan': 0.01, 'Netherlands': 0.01,
}
COUNTRY_LIST   = list(COUNTRIES.keys())
COUNTRY_WEIGHT = list(COUNTRIES.values())

COUNTRY_CODES = {
    'UAE': 'AE', 'Saudi Arabia': 'SA', 'USA': 'US', 'UK': 'GB', 'India': 'IN',
    'Australia': 'AU', 'Qatar': 'QA', 'Kuwait': 'KW', 'Oman': 'OM', 'Canada': 'CA',
    'Germany': 'DE', 'France': 'FR', 'Italy': 'IT', 'South Korea': 'KR', 'Japan': 'JP',
    'Netherlands': 'NL',
}

STATES_BY_COUNTRY = {
    'UAE':          ['Dubai', 'Abu Dhabi', 'Sharjah', 'Ajman', 'Ras Al Khaimah'],
    'Saudi Arabia': ['Riyadh', 'Jeddah', 'Dammam', 'Dhahran', 'Al Khobar', 'Jubail'],
    'USA':          ['Texas', 'California', 'Louisiana', 'Oklahoma', 'New York', 'Florida'],
    'UK':           ['England', 'Scotland', 'Wales', 'Northern Ireland'],
    'India':        ['Maharashtra', 'Gujarat', 'Rajasthan', 'Tamil Nadu', 'Karnataka'],
    'Australia':    ['Queensland', 'Western Australia', 'New South Wales', 'Victoria'],
    'Qatar':        ['Doha', 'Al Rayyan', 'Al Wakrah'],
    'Kuwait':       ['Kuwait City', 'Ahmadi', 'Hawally'],
    'Oman':         ['Muscat', 'Dhofar', 'Al Batinah'],
    'Canada':       ['Alberta', 'British Columbia', 'Ontario', 'Saskatchewan'],
    'Germany':      ['Bavaria', 'North Rhine-Westphalia', 'Hamburg', 'Berlin'],
    'France':       ['Île-de-France', 'Hauts-de-France', 'Auvergne-Rhône-Alpes'],
    'Italy':        ['Lombardy', 'Veneto', 'Emilia-Romagna', 'Lazio'],
    'South Korea':  ['Seoul', 'Busan', 'Incheon', 'Ulsan'],
    'Japan':        ['Tokyo', 'Osaka', 'Aichi', 'Kanagawa'],
    'Netherlands':  ['North Holland', 'South Holland', 'Utrecht'],
}

SECTOR_CODES = {
    'CIVIL_CONSTRUCTION': 'CIV', 'OIL_GAS': 'ONG', 'POWER': 'PWR', 'INFRASTRUCTURE': 'INF',
}

CONTRACT_MEDIANS = {
    'BUILDING': 5_000_000, 'HIGHWAY': 30_000_000, 'APARTMENT': 3_000_000,
    'BRIDGE': 20_000_000, 'REFINERY': 200_000_000, 'PIPELINE': 80_000_000,
    'DRILLING': 50_000_000, 'LNG': 500_000_000, 'POWER_THERMAL': 300_000_000,
    'POWER_SOLAR': 100_000_000, 'POWER_WIND': 150_000_000, 'SUBSTATION': 40_000_000,
    'PORT': 60_000_000, 'WATER_TREATMENT': 25_000_000,
}

LATLON = {
    'UAE': (24.5, 54.5), 'Saudi Arabia': (23.5, 45.0), 'USA': (30.0, -95.0),
    'UK': (52.0, -1.0), 'India': (19.0, 73.0), 'Australia': (-27.5, 153.0),
    'Qatar': (25.3, 51.5), 'Kuwait': (29.4, 47.9), 'Oman': (23.6, 58.5),
    'Canada': (51.0, -114.0), 'Germany': (50.1, 8.7), 'France': (48.8, 2.3),
    'Italy': (41.9, 12.5), 'South Korea': (35.2, 129.0), 'Japan': (35.7, 139.7),
    'Netherlands': (52.4, 4.9),
}

CLIENT_GOVTS = [
    'Ministry of Transport', 'Ministry of Energy', 'National Oil Company',
    'Public Works Department', 'Roads & Transport Authority', 'Water Authority',
    'Housing Authority', 'Ports Authority', 'National Gas Company',
    'Power & Water Corporation', 'Industrial Zones Authority',
]

CLIENT_CORPS = [
    'ADNOC', 'Saudi Aramco', 'Shell', 'BP', 'ExxonMobil', 'TotalEnergies',
    'Chevron', 'ENOC', 'QatarEnergy', 'KOC', 'PDO', 'Woodside Energy',
]

DISCIPLINES = ['CIVIL', 'STRUCTURAL', 'MECHANICAL', 'ELECTRICAL', 'PIPING',
               'INSTRUMENTATION', 'HVAC', 'SAFETY']


# ── Helpers ───────────────────────────────────────────────────────────────────
def rand_date(start_year=2019, end_year=2024):
    start = datetime(start_year, 1, 1)
    end   = datetime(end_year, 12, 31)
    return (start + timedelta(days=random.randint(0, (end - start).days))).strftime('%Y-%m-%d')


def project_name(ptype):
    adjectives = ['New', 'Phase II', 'Extension', 'Expansion', 'Upgrade', 'Development']
    locations  = [fake.city(), fake.city(), fake.state()]
    adj = random.choice(adjectives)
    loc = random.choice(locations)
    labels = {
        'BUILDING': f"{loc} {adj} Commercial Complex",
        'HIGHWAY': f"{loc} {adj} Highway Link",
        'APARTMENT': f"{loc} {adj} Residential Tower",
        'BRIDGE': f"{loc} {adj} Bridge Construction",
        'REFINERY': f"{loc} {adj} Refinery Revamp",
        'PIPELINE': f"{loc} {adj} Pipeline Installation",
        'DRILLING': f"{loc} {adj} Drilling Campaign",
        'LNG': f"{loc} {adj} LNG Train",
        'POWER_THERMAL': f"{loc} {adj} Thermal Power Plant",
        'POWER_SOLAR': f"{loc} {adj} Solar Farm",
        'POWER_WIND': f"{loc} {adj} Wind Farm",
        'SUBSTATION': f"{loc} {adj} Substation Upgrade",
        'PORT': f"{loc} {adj} Port Terminal",
        'WATER_TREATMENT': f"{loc} {adj} Water Treatment Plant",
    }
    return labels.get(ptype, f"{loc} {adj} Project")


# ── Main generation ──────────────────────────────────────────────────────────
def generate_projects(n):
    rows = []
    for i in range(n):
        ptype   = random.choices(PROJECT_TYPES, PROJECT_TYPE_W)[0]
        sector  = PROJECT_TYPE_SECTOR[ptype]
        sc      = SECTOR_CODES[sector]
        country = random.choices(COUNTRY_LIST, COUNTRY_WEIGHT)[0]
        cc      = COUNTRY_CODES[country]
        state   = random.choice(STATES_BY_COUNTRY.get(country, ['N/A']))

        year    = random.randint(2019, 2024)
        pid     = f"PRJ-{year}-{i+1:05d}"
        pcode   = f"{sc}-{cc}-{i+1:04d}"
        sub_sec = random.choice(SUB_SECTORS[sector])

        if sector == 'OIL_GAS':
            client = random.choice(CLIENT_CORPS)
        elif random.random() < 0.4:
            client = random.choice(CLIENT_GOVTS)
        else:
            client = fake.company()
        client_id = f"CLT-{random.randint(10000, 99999)}"

        status   = random.choices(STATUS_LIST, STATUS_W)[0]
        phase    = PHASE_MAP[status]
        priority = random.choices(PRIORITY_LIST, PRIORITY_W)[0]
        risk     = random.choices(RISK_LIST, RISK_W)[0]

        median_val = CONTRACT_MEDIANS[ptype]
        cv = float(np.random.lognormal(np.log(median_val), 0.6))
        cv = round(max(100_000, min(2_000_000_000, cv)), 2)

        if priority == 'CRITICAL':
            cv = max(cv, float(np.percentile([CONTRACT_MEDIANS[ptype]] * 10, 90)))
            cv = round(cv * random.uniform(1.2, 2.0), 2)

        approved_budget = round(cv * random.uniform(0.90, 1.05), 2)

        start_dt = datetime(year, random.randint(1, 12), random.randint(1, 28))
        dur_days = random.randint(180, 1800)
        planned_end = start_dt + timedelta(days=dur_days)
        actual_end = None
        if status == 'CLOSED':
            offset = random.randint(-90, 180)
            actual_end = (planned_end + timedelta(days=offset)).strftime('%Y-%m-%d')

        disc = random.choice(DISCIPLINES)
        wbs1 = f"{pcode}-{disc}"
        wbs2 = f"{wbs1}-{random.choice(['MAIN', 'AUX', 'TEMP'])}"
        wbs3 = f"{wbs2}-{random.randint(100, 999)}"

        base_lat, base_lon = LATLON.get(country, (25.0, 55.0))
        lat = round(base_lat + random.uniform(-2, 2), 6)
        lon = round(base_lon + random.uniform(-2, 2), 6)

        env_clearance = random.random() < 0.85
        created = rand_date(2018, year)

        rows.append({
            'project_id':              pid,
            'project_code':            pcode,
            'project_name':            project_name(ptype),
            'project_type':            ptype,
            'sector':                  sector,
            'sub_sector':              sub_sec,
            'client_name':             client,
            'client_id':               client_id,
            'project_manager':         fake.name(),
            'project_director':        fake.name(),
            'country':                 country,
            'state':                   state,
            'city':                    fake.city(),
            'contract_value_usd':      cv,
            'approved_budget_usd':     approved_budget,
            'project_status':          status,
            'start_date':              start_dt.strftime('%Y-%m-%d'),
            'planned_completion_date': planned_end.strftime('%Y-%m-%d'),
            'actual_completion_date':  actual_end,
            'phase':                   phase,
            'priority':                priority,
            'risk_level':              risk,
            'wbs_level_1':             wbs1,
            'wbs_level_2':             wbs2,
            'wbs_level_3':             wbs3,
            'environmental_clearance': env_clearance,
            'latitude':                lat,
            'longitude':               lon,
            'created_date':            created,
        })

    return pd.DataFrame(rows)


# ── Write to Excel ────────────────────────────────────────────────────────────
def main():
    print(f"  Generating {N:,} project records...", flush=True)
    df = generate_projects(N)

    print(f"  Writing to {OUT_FILE}...", flush=True)
    with pd.ExcelWriter(OUT_FILE, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='projects')
        meta = pd.DataFrame({
            'key':   ['source', 'records', 'generated_at', 'seed', 'schema_version'],
            'value': ['projects.xlsx', str(len(df)),
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
    assert df['project_id'].nunique() == N, "Duplicate project_ids"
    civil_types = {'BUILDING', 'HIGHWAY', 'APARTMENT', 'BRIDGE'}
    og_types    = {'REFINERY', 'PIPELINE', 'DRILLING', 'LNG'}
    pwr_types   = {'POWER_THERMAL', 'POWER_SOLAR', 'POWER_WIND', 'SUBSTATION'}
    assert df[df['project_type'].isin(civil_types)]['sector'].eq('CIVIL_CONSTRUCTION').all()
    assert df[df['project_type'].isin(og_types)]['sector'].eq('OIL_GAS').all()
    assert df[df['project_type'].isin(pwr_types)]['sector'].eq('POWER').all()
    assert df['project_status'].isin(STATUS_LIST).all()
    assert (df['contract_value_usd'] >= 100_000).all()
    assert (df['contract_value_usd'] <= 2_000_000_000).all()
    print(f"  Validation: PASSED")


if __name__ == '__main__':
    main()