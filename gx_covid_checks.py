"""
Great Expectations — COVID-19 Silver Layer Data Quality Suite
--------------------------------------------------------------
Runs 14 validation checks on the Silver Parquet dataset.
Categories: completeness, range, set membership, uniqueness, type, volume.

Usage:
    # Step 1: Download a Silver Parquet part file from S3
    # Windows:
    # aws s3 cp s3://covid-de-silver-YOUR_NAME/covid/covid_clean.parquet/ ^
    #   C:\\Temp\\silver_covid\\ --recursive --exclude "*" --include "part-0000*"

    # Step 2: Run validation
    pip install great_expectations pandas pyarrow
    python data_quality/gx_covid_checks.py

Exit code 0 = all pass | Exit code 1 = one or more failures (fails CI/CD)
"""

import sys
import os
import pandas as pd
import great_expectations as ge
from datetime import datetime

# ── Config ────────────────────────────────────────────────────────────────────
# Update this path to wherever you saved your Silver part file
SILVER_PATH = r'C:\Temp\silver_covid'

# ── Load Data ─────────────────────────────────────────────────────────────────
print('\n' + '=' * 58)
print('  COVID-19 Silver Layer — Data Quality Validation')
print('=' * 58 + '\n')

try:
    df = pd.read_parquet(SILVER_PATH)
except Exception as e:
    print(f'[ERROR] Could not load Silver Parquet: {e}')
    print('\nDownload a part file from S3 first:')
    print('aws s3 cp s3://covid-de-silver-YOUR_NAME/covid/covid_clean.parquet/')
    print('  C:\\Temp\\silver_covid\\ --recursive --exclude "*" --include "part-0000*"')
    sys.exit(1)

ge_df = ge.from_pandas(df)
print(f'[INFO] Loaded {len(df):,} rows  |  {len(df.columns)} columns\n')

results = []

def check(result, label):
    status = 'PASS' if result['success'] else 'FAIL'
    results.append({'check': label, 'status': status, 'run_time': datetime.utcnow().isoformat()})
    icon = 'PASS' if result['success'] else 'FAIL'
    print(f'  [{icon}]  {label}')

# ── 1. Completeness Checks ────────────────────────────────────────────────────
print('── Completeness ──────────────────────────────────────')
check(ge_df.expect_column_values_to_not_be_null('iso_code'),  'iso_code: no nulls')
check(ge_df.expect_column_values_to_not_be_null('location'),  'location: no nulls')
check(ge_df.expect_column_values_to_not_be_null('date'),      'date: no nulls')
check(ge_df.expect_column_values_to_not_be_null('continent'), 'continent: no nulls (aggregate rows filtered)')

# ── 2. Range Checks ───────────────────────────────────────────────────────────
print('\n── Range Validation ──────────────────────────────────')
check(ge_df.expect_column_values_to_be_between('new_cases',  0, 10_000_000),
      'new_cases: >= 0 (negative values fixed)')
check(ge_df.expect_column_values_to_be_between('new_deaths', 0, 100_000),
      'new_deaths: >= 0 (negative values fixed)')
check(ge_df.expect_column_values_to_be_between('death_rate', 0, 100, mostly=0.99),
      'death_rate: 0-100% (99% of rows)')
check(ge_df.expect_column_values_to_be_between('vaccination_rate', 0, 200, mostly=0.99),
      'vaccination_rate: 0-200% (boosters can exceed 100%)')

# ── 3. Set Membership Check ───────────────────────────────────────────────────
print('\n── Set Membership ────────────────────────────────────')
valid_continents = ['Africa', 'Asia', 'Europe', 'North America', 'Oceania', 'South America']
check(ge_df.expect_column_values_to_be_in_set('continent', valid_continents),
      'continent: only 6 valid values')

# ── 4. Uniqueness Check ───────────────────────────────────────────────────────
print('\n── Uniqueness ────────────────────────────────────────')
check(ge_df.expect_compound_columns_to_be_unique(['iso_code', 'date']),
      'iso_code + date: unique combination (no duplicate records)')

# ── 5. Type Checks ────────────────────────────────────────────────────────────
print('\n── Type Checks ───────────────────────────────────────')
check(ge_df.expect_column_values_to_be_of_type('new_cases',  'float64'), 'new_cases: float64')
check(ge_df.expect_column_values_to_be_of_type('new_deaths', 'float64'), 'new_deaths: float64')
check(ge_df.expect_column_values_to_be_of_type('population', 'float64'), 'population: float64')

# ── 6. Volume Check ───────────────────────────────────────────────────────────
print('\n── Volume ────────────────────────────────────────────')
check(ge_df.expect_table_row_count_to_be_between(100_000, 400_000),
      'row count: 100K-400K rows expected')

# ── Summary ───────────────────────────────────────────────────────────────────
passed = sum(1 for r in results if r['status'] == 'PASS')
failed = sum(1 for r in results if r['status'] == 'FAIL')
total  = len(results)

print(f'\n{"=" * 58}')
print(f'  RESULT: {passed}/{total} checks PASSED  |  {failed} FAILED')
print(f'{"=" * 58}\n')

# Save DQ report
os.makedirs('docs', exist_ok=True)
report = pd.DataFrame(results)
report.to_csv('docs/dq_report.csv', index=False)
print(f'[INFO] DQ report saved → docs/dq_report.csv')

if failed > 0:
    print(f'\n[ALERT] {failed} check(s) failed. Review docs/dq_report.csv.')
    sys.exit(1)
else:
    print('[INFO] All checks passed. Silver layer is clean and ready for Gold.')
    sys.exit(0)
