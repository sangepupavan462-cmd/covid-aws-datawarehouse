"""
COVID-19 EDA — Exploratory Data Analysis
------------------------------------------
Run locally before uploading to S3 Bronze.
Generates schema summary, null analysis, date range, and country coverage.

Usage:
    pip install pandas numpy pyarrow
    python eda_covid.py
"""

import pandas as pd
import numpy as np
import os

# Update this path if your file is in a different location
CSV_PATH = r'C:\covid-aws-datawarehouse\data\raw\owid-covid-data.csv'

print('Loading COVID-19 dataset...')
df = pd.read_csv(CSV_PATH, low_memory=False)

# ── Shape ─────────────────────────────────────────────────────────────────────
print(f'\n{"="*60}')
print(f'  SHAPE: {df.shape[0]:,} rows  |  {df.shape[1]} columns')
print(f'{"="*60}')

# ── Columns ───────────────────────────────────────────────────────────────────
print('\n── All Columns ───────────────────────────────────────')
for i, col in enumerate(df.columns, 1):
    print(f'  {i:2}. {col}')

# ── Data Types ────────────────────────────────────────────────────────────────
print('\n── Column Dtypes ─────────────────────────────────────')
print(df.dtypes.to_string())

# ── Null Analysis ─────────────────────────────────────────────────────────────
print('\n── Null Analysis (columns with nulls only) ───────────')
null_counts = df.isnull().sum()
null_pct    = (null_counts / len(df) * 100).round(2)
null_df = pd.DataFrame({'null_count': null_counts, 'null_pct_%': null_pct})
print(null_df[null_df['null_count'] > 0].sort_values('null_pct_%', ascending=False).to_string())

# ── Date Range ────────────────────────────────────────────────────────────────
print('\n── Date Range ────────────────────────────────────────')
df['date'] = pd.to_datetime(df['date'])
print(f'  Earliest date : {df["date"].min().date()}')
print(f'  Latest date   : {df["date"].max().date()}')
print(f'  Total days    : {(df["date"].max() - df["date"].min()).days:,}')

# ── Country Coverage ──────────────────────────────────────────────────────────
print('\n── Coverage ──────────────────────────────────────────')
print(f'  Unique locations  : {df["location"].nunique()}')
print(f'  Country rows only : {df[df["continent"].notna()]["location"].nunique()} (after filtering aggregates)')
print(f'\n  Continent breakdown:')
print(df[df['continent'].notna()]['continent'].value_counts().to_string())

# ── Aggregate Rows ────────────────────────────────────────────────────────────
print('\n── Aggregate / Non-Country Rows (to filter in Silver) ─')
agg_locs = df[df['continent'].isnull()]['location'].unique()
for loc in sorted(agg_locs):
    print(f'  {loc}')

# ── Numeric Summary ───────────────────────────────────────────────────────────
print('\n── Key Metric Summary ────────────────────────────────')
key_cols = ['new_cases', 'new_deaths', 'total_cases', 'total_deaths',
            'total_vaccinations', 'people_vaccinated_per_hundred',
            'icu_patients', 'hosp_patients']
available = [c for c in key_cols if c in df.columns]
print(df[available].describe().round(2).to_string())

# ── Negative Values Check ─────────────────────────────────────────────────────
print('\n── Negative Value Check ──────────────────────────────')
for col in ['new_cases', 'new_deaths']:
    if col in df.columns:
        neg_count = (df[col] < 0).sum()
        print(f'  {col}: {neg_count:,} negative rows (will be floored to 0 in Silver)')

# ── Save EDA Summary ──────────────────────────────────────────────────────────
os.makedirs(r'C:\covid-aws-datawarehouse\docs', exist_ok=True)
summary = pd.DataFrame({
    'column'      : df.columns,
    'dtype'       : df.dtypes.values,
    'null_count'  : null_counts.values,
    'null_pct'    : null_pct.values,
    'unique_count': df.nunique().values
})
summary.to_csv(r'C:\covid-aws-datawarehouse\docs\eda_summary.csv', index=False)
print(f'\n[INFO] EDA summary saved → docs/eda_summary.csv')
print(f'[INFO] EDA complete.')
