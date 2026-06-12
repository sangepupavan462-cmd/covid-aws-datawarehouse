-- ============================================================
-- COVID-19 Analytics — Redshift DDL + COPY Commands
-- Run in: Amazon Redshift Query Editor v2
-- Database: covid_analytics
-- ============================================================

-- Step 1: Create database (run once)
CREATE DATABASE covid_analytics;

-- Step 2: Switch to covid_analytics in the Query Editor dropdown
-- then run all CREATE TABLE statements below

-- ── Dimension Tables ──────────────────────────────────────────────────────────

CREATE TABLE dim_country (
    country_key              INTEGER      NOT NULL PRIMARY KEY,
    iso_code                 VARCHAR(10),
    country_name             VARCHAR(200),
    continent                VARCHAR(50),
    population               FLOAT,
    median_age               FLOAT,
    gdp_per_capita           FLOAT,
    human_development_index  FLOAT
)
DISTSTYLE ALL;  -- small table, replicate to all compute nodes

CREATE TABLE dim_date (
    date_key    INTEGER      NOT NULL PRIMARY KEY,
    date        DATE,
    year        INTEGER,
    month       INTEGER,
    quarter     INTEGER,
    month_name  VARCHAR(20),
    is_weekend  BOOLEAN,
    covid_wave  VARCHAR(50)
)
DISTSTYLE ALL;

CREATE TABLE dim_income_group (
    income_key    INTEGER     NOT NULL PRIMARY KEY,
    iso_code      VARCHAR(10),
    income_group  VARCHAR(30)
)
DISTSTYLE ALL;

-- ── Fact Table ────────────────────────────────────────────────────────────────

CREATE TABLE fact_covid_cases (
    country_key                          INTEGER   REFERENCES dim_country(country_key),
    date_key                             INTEGER   REFERENCES dim_date(date_key),
    income_key                           INTEGER   REFERENCES dim_income_group(income_key),
    new_cases                            FLOAT,
    new_deaths                           FLOAT,
    total_cases                          FLOAT,
    total_deaths                         FLOAT,
    new_cases_smoothed                   FLOAT,
    new_deaths_smoothed                  FLOAT,
    total_vaccinations                   FLOAT,
    people_vaccinated                    FLOAT,
    people_fully_vaccinated              FLOAT,
    people_vaccinated_per_hundred        FLOAT,
    people_fully_vaccinated_per_hundred  FLOAT,
    icu_patients                         FLOAT,
    hosp_patients                        FLOAT,
    death_rate                           FLOAT,
    vaccination_rate                     FLOAT,
    cases_per_million_calc               FLOAT,
    ingested_at                          TIMESTAMP
)
DISTKEY(country_key)   -- distribute by country for join performance
SORTKEY(date_key);     -- sort by date for time series query performance

-- ── COPY Commands — Load from S3 Gold ────────────────────────────────────────
-- Replace YOUR_ACCOUNT_ID with your 12-digit AWS Account ID
-- Get it: aws sts get-caller-identity --query Account --output text
-- Replace YOUR_NAME with your bucket suffix

COPY dim_country
FROM 's3://covid-de-gold-YOUR_NAME/dim_country/'
IAM_ROLE 'arn:aws:iam::YOUR_ACCOUNT_ID:role/CovidRedshiftRole'
FORMAT AS PARQUET;

COPY dim_date
FROM 's3://covid-de-gold-YOUR_NAME/dim_date/'
IAM_ROLE 'arn:aws:iam::YOUR_ACCOUNT_ID:role/CovidRedshiftRole'
FORMAT AS PARQUET;

COPY dim_income_group
FROM 's3://covid-de-gold-YOUR_NAME/dim_income_group/'
IAM_ROLE 'arn:aws:iam::YOUR_ACCOUNT_ID:role/CovidRedshiftRole'
FORMAT AS PARQUET;

COPY fact_covid_cases
FROM 's3://covid-de-gold-YOUR_NAME/fact_covid_cases/'
IAM_ROLE 'arn:aws:iam::YOUR_ACCOUNT_ID:role/CovidRedshiftRole'
FORMAT AS PARQUET;

-- ── Validation ────────────────────────────────────────────────────────────────

-- Row counts
SELECT 'fact_covid_cases'  AS table_name, COUNT(*) AS row_count FROM fact_covid_cases
UNION ALL SELECT 'dim_country',            COUNT(*) FROM dim_country
UNION ALL SELECT 'dim_date',               COUNT(*) FROM dim_date
UNION ALL SELECT 'dim_income_group',       COUNT(*) FROM dim_income_group
ORDER BY row_count DESC;

-- Quick analytics test — top 10 countries by total cases
SELECT
    c.country_name,
    c.continent,
    MAX(f.total_cases)   AS total_cases,
    MAX(f.total_deaths)  AS total_deaths,
    MAX(f.death_rate)    AS peak_death_rate_pct
FROM fact_covid_cases f
JOIN dim_country c ON f.country_key = c.country_key
GROUP BY c.country_name, c.continent
ORDER BY total_cases DESC
LIMIT 10;

-- Check for COPY errors if any COPY command fails
SELECT * FROM stl_load_errors ORDER BY starttime DESC LIMIT 10;
