# COVID-19 Global Analytics — AWS Cloud Data Warehouse

![AWS](https://img.shields.io/badge/AWS-232F3E?style=flat&logo=amazonaws&logoColor=white)
![Glue](https://img.shields.io/badge/AWS_Glue-FF9900?style=flat&logo=amazonaws&logoColor=white)
![Redshift](https://img.shields.io/badge/Redshift-8C4FFF?style=flat&logo=amazonaws&logoColor=white)
![PowerBI](https://img.shields.io/badge/Power_BI-F2C811?style=flat&logo=powerbi&logoColor=black)
![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=flat&logo=apachespark&logoColor=white)
![Terraform](https://img.shields.io/badge/Terraform-7B42BC?style=flat&logo=terraform&logoColor=white)

## Overview

End-to-end cloud data warehouse pipeline built on AWS, processing **300,000+ rows** of daily COVID-19 data from **200+ countries** through a **Medallion Architecture (Bronze → Silver → Gold)** using AWS Glue PySpark ETL, Amazon Redshift Serverless, and Power BI Desktop for analytics dashboards.

---

## Architecture

```
Source (Our World in Data — Daily CSV)
    → S3 Bronze       (Raw CSV — untouched)
    → AWS Lambda      (S3 Event-Driven Ingestion Trigger)
    → Glue ETL Job 1  (Bronze → Silver: Clean + Derived Metrics)
    → Glue ETL Job 2  (Silver → Gold: Star Schema)
    → Redshift        (Amazon Redshift Serverless — COPY from S3)
    → Power BI        (5-Panel Analytics Dashboard)
```

---

## Tech Stack

| Layer          | Technology                                      |
|----------------|-------------------------------------------------|
| Storage        | Amazon S3 (Bronze / Silver / Gold layers)       |
| Ingestion      | AWS Lambda + S3 Event Notifications             |
| ETL            | AWS Glue 4.0 (PySpark 3.3, Python 3.11)         |
| Catalog        | AWS Glue Data Catalog + Amazon Athena           |
| Data Warehouse | Amazon Redshift Serverless                      |
| Visualization  | Power BI Desktop (free)                         |
| Data Quality   | Great Expectations (14 checks)                  |
| IaC            | Terraform                                       |
| Language       | Python 3.11, SQL, PySpark                       |

---

## Dataset

- **Source**: [Our World in Data — COVID-19 Dataset](https://covid.ourworldindata.org/data/owid-covid-data.csv)
- **Size**: 300,000+ rows, 67 columns
- **Coverage**: 200+ countries, January 2020 to present (updated daily)
- **License**: Creative Commons — free for portfolio and personal use
- **Key Columns**: location, date, new_cases, new_deaths, total_vaccinations, people_vaccinated, icu_patients, hosp_patients, population, continent, gdp_per_capita

---

## Star Schema (Gold Layer)

```
fact_covid_cases  (300K+ rows — one row per country per date)
    ├── dim_country       (200+ countries: name, continent, population, GDP, HDI)
    ├── dim_date          (dates with COVID wave period labels: Wave 1–5)
    └── dim_income_group  (High / Upper-Middle / Lower-Middle / Low income)
```

---

## Silver Layer Transformations

| Rule | Description |
|------|-------------|
| Filter aggregate rows | Remove World, Europe, Asia etc. (continent IS NULL) |
| Filter OWID_ codes | Remove special aggregate iso_codes |
| Fix negative values | Floor new_cases and new_deaths at 0 (data corrections) |
| Type casting | Cast all 20 metric columns to FloatType explicitly |
| Date conversion | Convert string date to DateType |
| death_rate | total_deaths / total_cases * 100 |
| vaccination_rate | people_vaccinated / population * 100 |
| cases_per_million | total_cases / population * 1,000,000 |
| Wave period label | Assign Wave 1–5 based on date ranges |
| ingested_at | Processing timestamp for audit trail |

---

## Data Quality Results

| Category       | Checks | Result         |
|----------------|--------|----------------|
| Completeness   | 4      | ✅ PASS        |
| Range          | 4      | ✅ PASS        |
| Set Membership | 1      | ✅ PASS        |
| Uniqueness     | 1      | ✅ PASS        |
| Type           | 3      | ✅ PASS        |
| Volume         | 1      | ✅ PASS        |
| **Total**      | **14** | **14/14 PASS** |

---

## Dashboard (Power BI)

Key panels:
- **KPI Strip** — Total cases, total deaths, global death rate, avg vaccination rate, countries tracked
- **World Map** — Cases per million by country (bubble size = severity)
- **Line Chart** — Daily new cases over time by continent with country slicer
- **Bar Chart** — Cases and deaths by COVID wave period (Wave 1–5)
- **Scatter Plot** — Vaccination rate vs death rate (shows inverse correlation)

---

## Key Insights

- Countries in the **High Income** group had peak vaccination rates **3x higher** than Low Income countries
- Higher vaccination rates directly correlate with significantly **lower death rates** (visible in scatter plot)
- **Wave 5 (Omicron)** had the highest case counts but lower death rates vs Wave 4 (Delta)
- ICU data is only available for ~40 countries — primarily Western Europe and North America

---

## Setup & Run

### Prerequisites
- AWS account + AWS CLI v2 configured (`aws configure`)
- Python 3.9+ with pip
- Terraform 1.5+
- Power BI Desktop (free — download from powerbi.microsoft.com)
- Amazon Redshift ODBC Driver (for Power BI connection)

### 1. Clone the repo
```bash
git clone https://github.com/YOUR_USERNAME/covid-aws-datawarehouse.git
cd covid-aws-datawarehouse
```

### 2. Deploy infrastructure
```bash
cd terraform
terraform init
terraform plan
terraform apply    # type 'yes' to confirm
```

### 3. Download and upload COVID dataset
```bash
# Download the latest snapshot
curl -o owid-covid-data.csv https://covid.ourworldindata.org/data/owid-covid-data.csv

# Upload to S3 Bronze
aws s3 cp owid-covid-data.csv s3://covid-de-bronze-YOUR_NAME/raw/covid/year=2024/owid-covid-data.csv
```

### 4. Run Glue Crawlers and ETL Jobs
```
AWS Console → Glue → Crawlers
1. Run covid-bronze-crawler

AWS Console → Glue → ETL Jobs
2. Run covid-bronze-to-silver
3. Run covid-silver-to-gold
```

### 5. Load Redshift
```sql
-- Open Redshift Query Editor v2
-- Run: redshift/create_tables.sql
-- Update IAM role ARN in each COPY command
```

### 6. Run Data Quality Checks
```bash
pip install great_expectations pandas pyarrow
python data_quality/gx_covid_checks.py
# Expected: 14/14 PASS
```

### 7. Open Power BI Dashboard
```
1. Install Amazon Redshift ODBC driver
2. Open Power BI Desktop
3. Get Data → Amazon Redshift
4. Connect to your Redshift endpoint
5. Import all 4 tables and build dashboard
```

---

## AWS Cost Estimate

| Service            | Estimated Cost           |
|--------------------|--------------------------|
| Amazon S3          | < $0.50 / month          |
| AWS Glue Jobs      | < $2.00 total            |
| AWS Lambda         | $0.00 (free tier)        |
| Redshift Serverless| $3–$8 (pause when idle)  |
| Amazon Athena      | < $0.05 total            |
| Power BI Desktop   | $0.00 (free)             |
| **Total**          | **~$5–$12 for project**  |

> **Cost tip**: Pause Redshift Serverless when not actively querying. Run `terraform destroy` and empty S3 buckets when project is complete.

---

## Project Structure

```
covid-aws-datawarehouse/
├── README.md
├── .gitignore
├── architecture/
│   └── architecture_diagram.png    # Add your diagram here
├── terraform/
│   ├── main.tf
│   ├── variables.tf
│   └── outputs.tf
├── glue_jobs/
│   ├── bronze_to_silver.py
│   └── silver_to_gold.py
├── lambda/
│   └── ingestion_trigger.py
├── redshift/
│   ├── create_tables.sql
│   └── athena_queries.sql
├── data_quality/
│   └── gx_covid_checks.py
└── docs/
    ├── eda_summary.csv
    ├── dq_report.csv
    └── screenshots/
```

---

## Built By

**Pavan** | Data Engineer
AWS · PySpark · Redshift · Power BI · Terraform · Python
2025
