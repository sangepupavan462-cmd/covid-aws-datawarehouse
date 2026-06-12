"""
AWS Glue PySpark Job — Bronze to Silver
-----------------------------------------
Reads raw COVID-19 CSV from S3 Bronze.
Applies 10 transformation rules:
  1. Filter aggregate rows (World, Europe, etc.)
  2. Filter OWID_ special iso_codes
  3. Cast date to DateType
  4. Fix negative new_cases / new_deaths (floor to 0)
  5. Cast all 20 metric columns to FloatType
  6. Derive death_rate
  7. Derive vaccination_rate
  8. Derive cases_per_million_calc
  9. Extract record_year and record_month
  10. Add ingested_at timestamp
Writes clean Parquet to S3 Silver.

Job Parameters (set in Glue console):
  --bronze_bucket : covid-de-bronze-YOUR_NAME
  --silver_bucket : covid-de-silver-YOUR_NAME

Glue version : 4.0 (Spark 3.3, Python 3)
Worker type  : G.1X
Workers      : 2
"""

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    col, when, year, month,
    round as spark_round,
    current_timestamp
)
from pyspark.sql.types import FloatType, DateType

# ── Job Init ──────────────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'bronze_bucket', 'silver_bucket'])
sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args['JOB_NAME'], args)

BRONZE = args['bronze_bucket']
SILVER = args['silver_bucket']

print(f'[INFO] Bronze bucket : {BRONZE}')
print(f'[INFO] Silver bucket : {SILVER}')

# ── Read Raw CSV from Bronze ──────────────────────────────────────────────────
print('[INFO] Reading raw COVID CSV from Bronze S3...')
df_raw = (
    spark.read
    .option('header', 'true')
    .option('inferSchema', 'true')
    .csv(f's3://{BRONZE}/raw/covid/year=2024/owid-covid-data.csv')
)
print(f'[INFO] Raw row count : {df_raw.count():,}')

# ── Step 1 & 2: Filter out aggregate / non-country rows ──────────────────────
df = df_raw.filter(col('continent').isNotNull())
df = df.filter(~col('iso_code').startswith('OWID_'))
print(f'[INFO] After country filter : {df.count():,}')

# ── Step 3: Cast date column ──────────────────────────────────────────────────
df = df.withColumn('date', col('date').cast(DateType()))

# ── Step 4: Fix negative values (data correction artifacts) ──────────────────
for neg_col in ['new_cases', 'new_deaths', 'new_cases_smoothed', 'new_deaths_smoothed']:
    if neg_col in df.columns:
        df = df.withColumn(neg_col, when(col(neg_col) < 0, 0).otherwise(col(neg_col)))

# ── Step 5: Cast numeric columns to FloatType ─────────────────────────────────
float_cols = [
    'total_cases', 'new_cases', 'total_deaths', 'new_deaths',
    'new_cases_smoothed', 'new_deaths_smoothed',
    'total_cases_per_million', 'new_cases_per_million',
    'total_deaths_per_million', 'new_deaths_per_million',
    'total_vaccinations', 'people_vaccinated', 'people_fully_vaccinated',
    'people_vaccinated_per_hundred', 'people_fully_vaccinated_per_hundred',
    'icu_patients', 'hosp_patients', 'population',
    'median_age', 'aged_65_older', 'gdp_per_capita', 'human_development_index'
]
for c in float_cols:
    if c in df.columns:
        df = df.withColumn(c, col(c).cast(FloatType()))

# ── Steps 6–8: Derived metrics ────────────────────────────────────────────────
df = (
    df
    # death_rate: % of confirmed cases that resulted in death
    .withColumn('death_rate',
        when(
            col('total_cases').isNotNull() & (col('total_cases') > 0),
            spark_round(col('total_deaths') / col('total_cases') * 100, 4)
        ).otherwise(None))

    # vaccination_rate: % of population with at least one dose
    .withColumn('vaccination_rate',
        when(
            col('population').isNotNull() & (col('population') > 0),
            spark_round(col('people_vaccinated') / col('population') * 100, 2)
        ).otherwise(None))

    # cases_per_million: total cases scaled per 1M population
    .withColumn('cases_per_million_calc',
        when(
            col('population').isNotNull() & (col('population') > 0),
            spark_round(col('total_cases') / col('population') * 1_000_000, 2)
        ).otherwise(None))
)

# ── Step 9: Extract year and month ────────────────────────────────────────────
df = (
    df
    .withColumn('record_year',  year(col('date')))
    .withColumn('record_month', month(col('date')))
)

# ── Step 10: Add processing timestamp ────────────────────────────────────────
df = df.withColumn('ingested_at', current_timestamp())

# ── Select final Silver columns ───────────────────────────────────────────────
df_clean = df.select(
    'iso_code', 'location', 'continent', 'date',
    'record_year', 'record_month',
    'total_cases', 'new_cases', 'total_deaths', 'new_deaths',
    'new_cases_smoothed', 'new_deaths_smoothed',
    'total_cases_per_million', 'total_deaths_per_million',
    'total_vaccinations', 'people_vaccinated', 'people_fully_vaccinated',
    'people_vaccinated_per_hundred', 'people_fully_vaccinated_per_hundred',
    'icu_patients', 'hosp_patients', 'population',
    'median_age', 'gdp_per_capita', 'human_development_index',
    'death_rate', 'vaccination_rate', 'cases_per_million_calc',
    'ingested_at'
)

print(f'[INFO] Clean row count : {df_clean.count():,}')
print(f'[INFO] Columns         : {df_clean.columns}')

# ── Write Silver Parquet ──────────────────────────────────────────────────────
output_path = f's3://{SILVER}/covid/covid_clean.parquet'
print(f'[INFO] Writing Silver to : {output_path}')

df_clean.write.mode('overwrite').parquet(output_path)

print('[INFO] Bronze → Silver complete.')
job.commit()
