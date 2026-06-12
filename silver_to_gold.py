"""
AWS Glue PySpark Job — Silver to Gold (Star Schema)
------------------------------------------------------
Reads clean Parquet from S3 Silver.
Builds 4-table dimensional star schema:
  - fact_covid_cases   (one row per country per date)
  - dim_country        (200+ countries with HDI, GDP, population)
  - dim_date           (dates with COVID wave period labels Wave 1-5)
  - dim_income_group   (derived income classification from GDP per capita)
Writes all 4 tables as Parquet to S3 Gold.

Job Parameters (set in Glue console):
  --silver_bucket : covid-de-silver-YOUR_NAME
  --gold_bucket   : covid-de-gold-YOUR_NAME

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
    col, monotonically_increasing_id, when,
    year, month, quarter, date_format, dayofweek,
    current_timestamp
)

# ── Job Init ──────────────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'silver_bucket', 'gold_bucket'])
sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args['JOB_NAME'], args)

SILVER = args['silver_bucket']
GOLD   = args['gold_bucket']

print(f'[INFO] Silver bucket : {SILVER}')
print(f'[INFO] Gold bucket   : {GOLD}')

# ── Read Silver ───────────────────────────────────────────────────────────────
print('[INFO] Reading Silver Parquet...')
df = spark.read.parquet(f's3://{SILVER}/covid/covid_clean.parquet')
print(f'[INFO] Silver rows : {df.count():,}')

# ── dim_country ───────────────────────────────────────────────────────────────
print('[INFO] Building dim_country...')
dim_country = (
    df.select(
        'iso_code', 'location', 'continent', 'population',
        'median_age', 'gdp_per_capita', 'human_development_index'
    ).distinct()
    .withColumnRenamed('location', 'country_name')
    .withColumn('country_key', (monotonically_increasing_id() + 1).cast('int'))
    .select('country_key', 'iso_code', 'country_name', 'continent',
            'population', 'median_age', 'gdp_per_capita', 'human_development_index')
)

# ── dim_income_group (derived from GDP per capita) ────────────────────────────
print('[INFO] Building dim_income_group...')
dim_income = (
    dim_country.select('iso_code', 'gdp_per_capita').distinct()
    .withColumn('income_group',
        when(col('gdp_per_capita') >= 12000, 'High Income')
       .when(col('gdp_per_capita') >= 4000,  'Upper-Middle Income')
       .when(col('gdp_per_capita') >= 1000,  'Lower-Middle Income')
       .when(col('gdp_per_capita').isNotNull(), 'Low Income')
       .otherwise('Unknown'))
    .withColumn('income_key', (monotonically_increasing_id() + 1).cast('int'))
    .select('income_key', 'iso_code', 'income_group')
)

# ── dim_date (with COVID wave period labels) ──────────────────────────────────
print('[INFO] Building dim_date...')
dim_date = (
    df.select('date').distinct()
    .withColumn('year',       year(col('date')))
    .withColumn('month',      month(col('date')))
    .withColumn('quarter',    quarter(col('date')))
    .withColumn('month_name', date_format(col('date'), 'MMMM'))
    .withColumn('is_weekend', dayofweek(col('date')).isin([1, 7]))
    .withColumn('covid_wave',
        when(col('date') < '2020-07-01', 'Wave 1 - Initial Outbreak')
       .when(col('date') < '2021-01-01', 'Wave 2 - Second Surge')
       .when(col('date') < '2021-07-01', 'Wave 3 - Alpha Variant')
       .when(col('date') < '2022-01-01', 'Wave 4 - Delta Variant')
       .otherwise('Wave 5 - Omicron & Endemic'))
    .withColumn('date_key', (monotonically_increasing_id() + 1).cast('int'))
    .select('date_key', 'date', 'year', 'month', 'quarter',
            'month_name', 'is_weekend', 'covid_wave')
)

# ── fact_covid_cases (join all dimension keys) ────────────────────────────────
print('[INFO] Building fact_covid_cases...')
df_fact = (
    df
    .join(dim_country.select('iso_code', 'country_key'),
          on='iso_code', how='left')
    .join(dim_date.select('date', 'date_key'),
          on='date', how='left')
    .join(dim_income.select('iso_code', 'income_key'),
          on='iso_code', how='left')
)

fact_covid = df_fact.select(
    'country_key', 'date_key', 'income_key',
    'new_cases', 'new_deaths', 'total_cases', 'total_deaths',
    'new_cases_smoothed', 'new_deaths_smoothed',
    'total_vaccinations', 'people_vaccinated', 'people_fully_vaccinated',
    'people_vaccinated_per_hundred', 'people_fully_vaccinated_per_hundred',
    'icu_patients', 'hosp_patients',
    'death_rate', 'vaccination_rate', 'cases_per_million_calc',
    'ingested_at'
)

print(f'[INFO] fact_covid_cases rows : {fact_covid.count():,}')
print(f'[INFO] dim_country rows      : {dim_country.count():,}')
print(f'[INFO] dim_date rows         : {dim_date.count():,}')
print(f'[INFO] dim_income rows       : {dim_income.count():,}')

# ── Write Gold Parquet ────────────────────────────────────────────────────────
def write_gold(df, name):
    path = f's3://{GOLD}/{name}/'
    df.coalesce(1).write.mode('overwrite').parquet(path)
    print(f'[INFO] Written → {path}')

write_gold(fact_covid,  'fact_covid_cases')
write_gold(dim_country, 'dim_country')
write_gold(dim_date,    'dim_date')
write_gold(dim_income,  'dim_income_group')

print('[INFO] Silver → Gold complete.')
job.commit()
