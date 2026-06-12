-- ============================================================
-- COVID-19 Analytics — Athena Query Collection
-- Run in: Amazon Athena Query Editor
-- Database: covid_gold_db (created by Glue Crawler on Gold S3)
-- Output location: s3://covid-de-gold-YOUR_NAME/athena-results/
-- ============================================================

-- Query 1: Top 20 Countries by Total Cases
SELECT
    c.country_name,
    c.continent,
    MAX(f.total_cases)                      AS total_cases,
    MAX(f.total_deaths)                     AS total_deaths,
    ROUND(MAX(f.cases_per_million_calc), 0) AS cases_per_million
FROM fact_covid_cases f
JOIN dim_country c ON f.country_key = c.country_key
GROUP BY c.country_name, c.continent
ORDER BY total_cases DESC
LIMIT 20;

-- Query 2: Monthly New Cases by Continent
SELECT
    d.year,
    d.month,
    c.continent,
    SUM(f.new_cases)   AS monthly_new_cases,
    SUM(f.new_deaths)  AS monthly_new_deaths
FROM fact_covid_cases f
JOIN dim_country c ON f.country_key = c.country_key
JOIN dim_date d    ON f.date_key    = d.date_key
GROUP BY d.year, d.month, c.continent
ORDER BY d.year, d.month, monthly_new_cases DESC;

-- Query 3: Vaccination Rate vs Death Rate by Country
SELECT
    c.country_name,
    c.continent,
    ROUND(MAX(f.vaccination_rate), 2)  AS max_vax_rate_pct,
    ROUND(MAX(f.death_rate), 4)        AS peak_death_rate_pct,
    MAX(f.total_cases)                 AS total_cases
FROM fact_covid_cases f
JOIN dim_country c ON f.country_key = c.country_key
WHERE f.vaccination_rate IS NOT NULL
GROUP BY c.country_name, c.continent
ORDER BY max_vax_rate_pct DESC
LIMIT 20;

-- Query 4: Cases and Deaths by COVID Wave Period
SELECT
    d.covid_wave,
    SUM(f.new_cases)               AS total_new_cases,
    SUM(f.new_deaths)              AS total_new_deaths,
    COUNT(DISTINCT f.country_key)  AS countries_reporting
FROM fact_covid_cases f
JOIN dim_date d ON f.date_key = d.date_key
WHERE f.new_cases IS NOT NULL
GROUP BY d.covid_wave
ORDER BY total_new_cases DESC;

-- Query 5: Peak ICU Occupancy by Country (Top 15)
SELECT
    c.country_name,
    c.continent,
    ROUND(MAX(f.icu_patients), 0)   AS peak_icu_patients,
    ROUND(MAX(f.hosp_patients), 0)  AS peak_hosp_patients,
    MAX(f.total_cases)              AS total_cases
FROM fact_covid_cases f
JOIN dim_country c ON f.country_key = c.country_key
WHERE f.icu_patients IS NOT NULL
GROUP BY c.country_name, c.continent
ORDER BY peak_icu_patients DESC
LIMIT 15;

-- Query 6: COVID Impact by Income Group
SELECT
    i.income_group,
    COUNT(DISTINCT f.country_key)    AS countries,
    SUM(f.new_cases)                 AS total_cases,
    SUM(f.new_deaths)                AS total_deaths,
    ROUND(AVG(f.vaccination_rate),2) AS avg_vax_rate_pct
FROM fact_covid_cases f
JOIN dim_income_group i ON f.income_key = i.income_key
WHERE f.new_cases IS NOT NULL
GROUP BY i.income_group
ORDER BY total_cases DESC;

-- Query 7: Monthly Peak Cases per Country (Best Month)
SELECT
    c.country_name,
    d.year,
    d.month_name,
    SUM(f.new_cases)  AS monthly_cases
FROM fact_covid_cases f
JOIN dim_country c ON f.country_key = c.country_key
JOIN dim_date d    ON f.date_key    = d.date_key
WHERE f.new_cases IS NOT NULL
GROUP BY c.country_name, d.year, d.month_name
ORDER BY monthly_cases DESC
LIMIT 20;
