# Lending Analytics dbt Project

A production-style analytics engineering project using **dbt + Snowflake** to transform raw lending data into clean, scalable datasets that enable faster, more reliable decision-making across Product, Risk, and Analytics teams.

## Project Overview

This project models end-to-end loan lifecycle data — from raw application and payment records through to business-ready fact tables and analytics marts. The goal is to eliminate bottlenecks by delivering clean datasets analysts can trust and use independently, without waiting on ad hoc data requests.

**Stack:** dbt Core | Snowflake | SQL | Python | Git

## Architecture

```
Raw Sources (Snowflake)
    └── raw_lending.loan_applications
    └── raw_lending.payment_transactions
    └── raw_lending.borrower_profiles
            │
            ▼
    Staging Layer (views)
    └── stg_loans          ← cleaned loan data, type casts, null handling
    └── stg_payments       ← cleaned payments, derived payment_health flag
    └── stg_borrowers      ← cleaned profiles, credit_segment classification
            │
            ▼
    Marts Layer (tables)
    └── fct_loan_performance   ← core fact table joining all sources
    └── mrt_product_metrics    ← monthly KPIs for Product team
    └── mrt_risk_summary       ← risk cohort analysis for Risk team
```

## Models

### Staging Layer (`models/staging/`)
| Model | Description |
|-------|-------------|
| `stg_loans` | Cleans raw loan applications — casts types, removes invalid records, standardizes status values |
| `stg_payments` | Cleans payment transactions — derives `payment_health` flag (current/late/delinquent) |
| `stg_borrowers` | Cleans borrower profiles — segments borrowers by credit quality (prime/near_prime/subprime/deep_subprime) |

### Marts Layer (`models/marts/`)
| Model | Description | Used By |
|-------|-------------|---------|
| `fct_loan_performance` | Core fact table — loan metrics, repayment rates, risk flags | All teams |
| `mrt_product_metrics` | Monthly product KPIs — conversion rates, volume, funnel metrics | Product team |
| `mrt_risk_summary` | Risk cohort analysis — delinquency rates, high-risk exposure, risk-adjusted yield | Risk team |

## Data Quality Tests

All models include dbt tests for:
- `not_null` — critical ID and metric fields
- `unique` — primary keys across all models
- `accepted_values` — loan_status, payment_health, credit_segment enums
- `relationships` — referential integrity between payment and loan records

Run tests:
```bash
dbt test
```

## Key Business Metrics

| Metric | Location | Description |
|--------|----------|-------------|
| `funding_conversion_rate` | `mrt_product_metrics` | % of applications funded |
| `repayment_rate_pct` | `fct_loan_performance` | % of loan repaid to date |
| `delinquency_rate` | `mrt_risk_summary` | % of loans past due by segment |
| `risk_adjusted_yield` | `mrt_risk_summary` | Interest rate adjusted for default risk |
| `is_high_risk` | `fct_loan_performance` | Boolean flag for underwriting decisions |
| `loan_health_status` | `fct_loan_performance` | current / delinquent / paid_off / charged_off_risk |

## Setup & Usage

### Prerequisites
- dbt Core >= 1.7
- Snowflake account (free trial available)
- Python >= 3.9

### Installation
```bash
git clone https://github.com/Akanksha576/dbt-lending-analytics
cd dbt-lending-analytics
pip install -r requirements.txt
dbt deps
```

### Configure Snowflake connection
```bash
cp profiles.yml ~/.dbt/profiles.yml
# Update with your Snowflake credentials
```

### Run the project
```bash
dbt run --select staging    # Run staging models only
dbt run --select marts      # Run mart models only
dbt run                     # Run all models
dbt test                    # Run all data quality tests
dbt docs generate           # Generate documentation
dbt docs serve              # View docs in browser
```

## Design Decisions

**Why staging views?** Staging models are materialized as views to avoid storing redundant data while still providing a clean interface to raw sources. Marts are tables for query performance.

**Why separate Product and Risk marts?** Product and Risk teams have different analytical needs and query patterns. Separate marts enable self-service analytics without cross-team data bottlenecks.

**Data quality approach:** Raw data often contains mixed types, empty strings, and nulls. All staging models use `NULLIF`, explicit `CAST`, and `WHERE` filters to ensure downstream models receive clean, typed data.

## Project Structure

```
├── dbt_project.yml
├── packages.yml
├── requirements.txt
├── models/
│   ├── staging/
│   │   ├── schema.yml          # Source definitions + staging tests
│   │   ├── stg_loans.sql
│   │   ├── stg_payments.sql
│   │   └── stg_borrowers.sql
│   └── marts/
│       ├── schema.yml          # Mart model docs + tests
│       ├── fct_loan_performance.sql
│       ├── mrt_product_metrics.sql
│       └── mrt_risk_summary.sql
├── macros/
│   └── generate_schema_name.sql
└── tests/
```
