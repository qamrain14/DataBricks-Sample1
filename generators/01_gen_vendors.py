"""
Generator 01: Vendor Master
============================
Output : data/raw/vendors.xlsx
Records: 100,000
Seed   : 42
Domain : Master Data — Procurement

Schema columns: See .github/copilot-instructions.md § GENERATOR 01

Business rules enforced:
  - BLACKLISTED vendors have performance_rating < 2.0
  - APPROVED prequalification → performance_rating > 2.5
  - Small businesses have credit_limit_usd < 200,000
  - OEM vendors are always iso_certified = True
  - OIL_GAS sector vendors: hse_rating minimum 3

Usage (standalone):
    python 01_gen_vendors.py
    OUTPUT_DIR=/path/to/dir N_RECORDS=100000 python 01_gen_vendors.py
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
OUT_FILE  = os.path.join(OUT_DIR, 'vendors.xlsx')
SEED      = 42

fake = Faker(['en_US', 'en_GB'])
Faker.seed(SEED)
np.random.seed(SEED)
random.seed(SEED)

os.makedirs(OUT_DIR, exist_ok=True)

# ── Lookup tables ─────────────────────────────────────────────────────────────
VENDOR_TYPES = ['SUBCONTRACTOR','SUPPLIER','CONSULTANT','LABOUR_CONTRACTOR','OEM','MANUFACTURER']
VENDOR_TYPE_W = [0.25, 0.30, 0.10, 0.15, 0.10, 0.10]

SECTORS = ['CIVIL', 'OIL_GAS', 'POWER', 'MULTI']
SECTOR_W = [0.35, 0.30, 0.20, 0.15]

# Country distribution: construction/O&G focused geography
COUNTRIES = {
    'UAE':           0.20, 'Saudi Arabia':  0.18, 'USA':        0.14,
    'UK':            0.09, 'India':         0.08, 'Australia':  0.05,
    'Qatar':         0.05, 'Kuwait':        0.04, 'Oman':       0.04,
    'Canada':        0.03, 'Germany':       0.03, 'France':     0.02,
    'Italy':         0.02, 'South Korea':   0.01, 'Japan':      0.01,
    'Netherlands':   0.01,
}
COUNTRY_LIST   = list(COUNTRIES.keys())
COUNTRY_WEIGHT = list(COUNTRIES.values())

STATES_BY_COUNTRY = {
    'UAE':          ['Dubai','Abu Dhabi','Sharjah','Ajman','Ras Al Khaimah'],
    'Saudi Arabia': ['Riyadh','Jeddah','Dammam','Dhahran','Al Khobar','Jubail'],
    'USA':          ['Texas','California','Louisiana','Oklahoma','New York','Florida'],
    'UK':           ['England','Scotland','Wales','Northern Ireland'],
    'India':        ['Maharashtra','Gujarat','Rajasthan','Tamil Nadu','Karnataka'],
    'Australia':    ['Queensland','Western Australia','New South Wales','Victoria'],
    'Qatar':        ['Doha','Al Rayyan','Al Wakrah'],
    'Kuwait':       ['Kuwait City','Ahmadi','Hawally'],
    'Oman':         ['Muscat','Dhofar','Al Batinah'],
    'Canada':       ['Alberta','British Columbia','Ontario','Saskatchewan'],
    'Germany':      ['Bavaria','North Rhine-Westphalia','Hamburg','Berlin'],
    'France':       ['Île-de-France','Hauts-de-France','Auvergne-Rhône-Alpes'],
    'Italy':        ['Lombardy','Veneto','Emilia-Romagna','Lazio'],
    'South Korea':  ['Seoul','Busan','Incheon','Ulsan'],
    'Japan':        ['Tokyo','Osaka','Aichi','Kanagawa'],
    'Netherlands':  ['North Holland','South Holland','Utrecht'],
}

RISK_CAT    = ['LOW','MEDIUM','HIGH','BLACKLISTED']
RISK_W      = [0.50, 0.35, 0.13, 0.02]

PREQ_STATUS = ['APPROVED','PENDING','EXPIRED','REJECTED']
PREQ_W      = [0.70, 0.15, 0.10, 0.05]

PAY_TERMS   = [30, 45, 60, 90]
PAY_TERMS_W = [0.20, 0.30, 0.35, 0.15]

BANKS = ['HSBC','Citibank','JPMorgan Chase','Standard Chartered','ABN AMRO',
         'Deutsche Bank','BNP Paribas','Barclays','Wells Fargo','Bank of America']

COUNTRY_CODES = {
    'UAE':'AE','Saudi Arabia':'SA','USA':'US','UK':'GB','India':'IN',
    'Australia':'AU','Qatar':'QA','Kuwait':'KW','Oman':'OM','Canada':'CA',
    'Germany':'DE','France':'FR','Italy':'IT','South Korea':'KR','Japan':'JP',
    'Netherlands':'NL',
}

# ── Helper functions ──────────────────────────────────────────────────────────
def rand_date(start_year=2015, end_year=2022) -> str:
    start = datetime(start_year, 1, 1)
    end   = datetime(end_year, 12, 31)
    return (start + timedelta(days=random.randint(0, (end - start).days))).strftime('%Y-%m-%d')

def rand_future_date(years_ahead=3) -> str:
    start = datetime.now()
    end   = start + timedelta(days=years_ahead * 365)
    return (start + timedelta(days=random.randint(0, (end - start).days))).strftime('%Y-%m-%d')

def rand_last_updated() -> str:
    return rand_date(2022, 2024)

def iban(country_code: str) -> str:
    """Generate fake IBAN-style account number."""
    digits = ''.join([str(random.randint(0, 9)) for _ in range(16)])
    return f"{country_code}{random.randint(10,99)}{digits}"

def swift(bank: str, country_code: str) -> str:
    """Generate fake SWIFT/BIC code."""
    bank_code = bank[:4].upper().replace(' ', 'X')
    return f"{bank_code}{country_code}XXX"

# ── Main generation ────────────────────────────────────────────────────────────
def generate_vendors(n: int) -> pd.DataFrame:
    rows = []

    for i in range(n):
        vendor_id   = f"VND-{i+1:06d}"
        vendor_code = f"V{i+1:05d}"
        
        # Type and sector
        vtype  = random.choices(VENDOR_TYPES, VENDOR_TYPE_W)[0]
        sector = random.choices(SECTORS, SECTOR_W)[0]
        
        # Country / geography
        country    = random.choices(COUNTRY_LIST, COUNTRY_WEIGHT)[0]
        cc         = COUNTRY_CODES.get(country, 'XX')
        state_list = STATES_BY_COUNTRY.get(country, ['N/A'])
        state      = random.choice(state_list)
        city       = fake.city()
        
        # Risk and prequalification (apply business rules)
        risk     = random.choices(RISK_CAT, RISK_W)[0]
        preq_raw = random.choices(PREQ_STATUS, PREQ_W)[0]
        
        # Performance rating with business rules
        if risk == 'BLACKLISTED':
            perf = round(random.uniform(1.0, 1.9), 1)
            preq = 'REJECTED'
        elif risk == 'HIGH':
            perf = round(random.uniform(1.5, 3.0), 1)
            preq = preq_raw if preq_raw != 'APPROVED' else 'CONDITIONAL'
        else:
            perf = round(max(2.5, min(5.0, np.random.normal(3.5, 0.7))), 1)
            preq = preq_raw
        
        if preq == 'APPROVED' and perf < 2.5:
            perf = 2.5
        
        # Credit limit with business rules
        small_biz = (vtype in ['SUBCONTRACTOR','LABOUR_CONTRACTOR'] and 
                     random.random() < 0.30)
        if small_biz:
            credit = round(random.uniform(10_000, 200_000), 2)
        else:
            credit = round(max(10_000, min(5_000_000, 
                               np.random.lognormal(np.log(500_000), 0.8))), 2)
        
        # ISO certification — OEM always certified
        iso = True if vtype == 'OEM' else (random.random() < 0.60)
        
        # HSE rating — O&G sector: minimum 3
        if sector == 'OIL_GAS':
            hse = random.choices([3, 4, 5], [0.30, 0.45, 0.25])[0]
        else:
            hse = random.choices([1, 2, 3, 4, 5], [0.05, 0.15, 0.35, 0.30, 0.15])[0]
        
        # Banking
        bank       = random.choice(BANKS)
        account    = iban(cc)
        swift_code = swift(bank, cc)
        
        # Contact
        contact    = fake.name()
        email_user = contact.lower().replace(' ', '.').replace("'", '')[:20]
        co_domain  = fake.domain_name()
        email      = f"{email_user}@{co_domain}"
        
        # Registration
        reg_num    = f"REG-{cc}-{random.randint(10_000_000, 99_999_999)}"
        vat_num    = f"VAT{random.randint(100_000_000_000_000, 999_999_999_999_999)}"
        tax_id     = f"TID-{random.randint(100_000_000, 999_999_999)}"
        
        created    = rand_date(2015, 2022)
        updated    = rand_last_updated()
        preq_expiry = rand_future_date(3) if preq == 'APPROVED' else rand_date(2020, 2023)
        
        turnover   = round(max(50_000, np.random.lognormal(np.log(2_000_000), 1.2)), 2)
        years_biz  = random.randint(5, 50)
        pay_terms  = random.choices(PAY_TERMS, PAY_TERMS_W)[0]
        
        legal_name = fake.company() + (' LLC' if country in ['UAE','Saudi Arabia'] 
                     else ' Ltd.' if country in ['UK','India','Australia']
                     else ' Inc.' if country == 'USA' else ' GmbH' if country == 'Germany'
                     else ' S.A.' if country in ['France','Italy'] else ' Corp.')

        rows.append({
            'vendor_id':                  vendor_id,
            'vendor_code':                vendor_code,
            'vendor_name':                fake.company(),
            'vendor_legal_name':          legal_name,
            'vendor_type':                vtype,
            'sector_specialization':      sector,
            'country':                    country,
            'state':                      state,
            'city':                       city,
            'postal_code':                fake.postcode(),
            'address_line1':              fake.street_address(),
            'address_line2':              (fake.secondary_address() 
                                           if random.random() < 0.3 else None),
            'registration_number':        reg_num,
            'vat_number':                 vat_num,
            'tax_id':                     tax_id,
            'bank_name':                  bank,
            'bank_account':               account,
            'swift_code':                 swift_code,
            'payment_terms_days':         pay_terms,
            'credit_limit_usd':           credit,
            'performance_rating':         perf,
            'risk_category':              risk,
            'prequalification_status':    preq,
            'prequalification_expiry_date': preq_expiry,
            'iso_certified':              iso,
            'hse_rating':                 hse,
            'minority_owned':             random.random() < 0.10,
            'small_business':             small_biz,
            'contact_name':               contact,
            'contact_email':              email,
            'contact_phone':              fake.phone_number(),
            'annual_turnover_usd':        turnover,
            'years_in_business':          years_biz,
            'created_date':               created,
            'last_updated_date':          updated,
        })

    return pd.DataFrame(rows)

# ── Write to Excel ────────────────────────────────────────────────────────────
def main():
    print(f"  Generating {N:,} vendor records...", flush=True)
    df = generate_vendors(N)
    
    print(f"  Writing to {OUT_FILE}...", flush=True)
    with pd.ExcelWriter(OUT_FILE, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='vendors')
        # Add a metadata sheet
        meta = pd.DataFrame({
            'key':   ['source', 'records', 'generated_at', 'seed', 'schema_version'],
            'value': ['vendors.xlsx', str(len(df)), 
                      datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 
                      str(SEED), '1.0']
        })
        meta.to_excel(writer, index=False, sheet_name='_metadata')
    
    size_mb = os.path.getsize(OUT_FILE) / 1_048_576
    print(f"  OUTPUT: {OUT_FILE}")
    print(f"  ROWS: {len(df)}")
    print(f"  SIZE: {size_mb:.1f} MB")
    
    # Quick validation
    assert len(df) == N, f"Expected {N} rows, got {len(df)}"
    assert df['vendor_id'].nunique() == N, "Duplicate vendor_ids found"
    assert df['risk_category'].isin(['LOW','MEDIUM','HIGH','BLACKLISTED']).all()
    blacklisted = df[df['risk_category'] == 'BLACKLISTED']
    assert (blacklisted['performance_rating'] < 2.0).all(), "Business rule violated"
    print(f"  Validation: PASSED")

if __name__ == '__main__':
    main()
