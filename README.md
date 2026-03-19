# Commodity Trading Sanctions Risk Analytics Platform

> Enterprise-grade Snowflake data platform for real-time sanctions risk screening, trade surveillance, and compliance analytics across commodity trading operations.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                                      │
│  ETRM/CTRM │ AIS Feeds │ KYC Systems │ OFAC/EU/UN │ Market Data        │
└──────┬──────┴─────┬─────┴──────┬──────┴──────┬─────┴──────┬─────────────┘
       │            │            │             │            │
       ▼            ▼            ▼             ▼            ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  INGESTION LAYER                                                         │
│  Snowpipe (auto-ingest) │ Snowpipe Streaming │ COPY INTO (bulk)         │
└──────────────────────────────────┬──────────────────────────────────────┘
                                   │
       ┌───────────────────────────┼───────────────────────────┐
       ▼                           ▼                           ▼
┌──────────────┐         ┌─────────────────┐         ┌─────────────────┐
│  BRONZE/RAW  │         │  SILVER/CURATED │         │  GOLD/ANALYTICS │
│  RAW schema  │ ──dbt──▶│  STAGING schema │ ──dbt──▶│  ANALYTICS      │
│  Raw tables  │         │  Staging views  │         │  Facts & Dims   │
│  Streams     │         │  Intermediates  │         │  Aggregates     │
└──────────────┘         └─────────────────┘         └────────┬────────┘
                                                              │
                                   ┌──────────────────────────┤
                                   ▼                          ▼
                          ┌─────────────────┐       ┌─────────────────┐
                          │   ML / GenAI    │       │   CONSUMPTION   │
                          │   ML schema     │       │   Streamlit     │
                          │   Feature store │       │   Dashboards    │
                          └─────────────────┘       └─────────────────┘
```

## Data Volume at Production Scale

| Dataset              | Daily Volume  | Total Records | Storage    |
|----------------------|---------------|---------------|------------|
| Vessel Movements     | 10M/day       | 3.65B         | ~2.5 TB    |
| Trades               | 2.5M/day      | 912.5M        | ~500 GB    |
| Screening Results    | 375K/day      | 136.9M        | ~75 GB     |
| Counterparties       | -             | 5M            | ~5 GB      |
| Vessels              | -             | 500K          | ~500 MB    |
| Sanctioned Entities  | -             | 250K          | ~250 MB    |

## Project Structure

```
sanctions-risk-platform/
│
├── snowflake/                          # Snowflake DDL & setup scripts
│   ├── 001_account_setup.sql           # Account config, databases, warehouses, RBAC
│   ├── 002_external_stages.sql         # File formats, internal & external stages
│   ├── 003_raw_tables.sql              # Raw table definitions with governance tags
│   ├── 004_snowpipe_setup.sql          # Snowpipe definitions for auto-ingestion
│   └── 005_streaming_setup.sql         # Streams, tasks, dynamic tables for CDC
│
├── sanctions_data_generator/           # Synthetic data generation (Python)
│   ├── config.py                       # Configuration dataclasses
│   ├── generators/                     # Data generator modules
│   │   ├── base_generator.py           # Abstract base with Parquet output
│   │   ├── counterparty_generator.py   # KYC counterparty data
│   │   ├── sanctions_list_generator.py # OFAC/EU/UN sanctions entries
│   │   ├── vessel_generator.py         # Vessel master data
│   │   ├── vessel_movement_generator.py# AIS position data (BILLIONS scale)
│   │   ├── trade_generator.py          # Trade transactions
│   │   └── screening_generator.py      # Screening results
│   ├── generate_all_data.py            # Master orchestrator
│   ├── load_to_snowflake.py            # Snowflake PUT + COPY INTO loader
│   ├── requirements.txt                # Python dependencies
│   └── .env.example                    # Environment variable template
│
├── sanctions_dbt/                      # DBT transformation project
│   ├── dbt_project.yml                 # Project configuration
│   ├── packages.yml                    # Package dependencies
│   ├── profiles.yml                    # Connection profiles
│   ├── macros/                         # Reusable SQL macros
│   │   ├── generate_schema_name.sql    # Schema routing per environment
│   │   ├── grant_select_on_schemas.sql # RBAC grants post-hook
│   │   ├── audit_columns.sql           # Standard audit columns
│   │   ├── hash_surrogate_key.sql      # Deterministic surrogate keys
│   │   └── test_macros/                # Custom test macros
│   ├── models/
│   │   ├── staging/                    # Bronze → Silver (views)
│   │   │   ├── stg_counterparties.sql
│   │   │   ├── stg_sanctions_lists.sql
│   │   │   ├── stg_vessels.sql
│   │   │   ├── stg_trades.sql
│   │   │   ├── stg_vessel_movements.sql
│   │   │   └── stg_screening_results.sql
│   │   ├── intermediate/              # Silver enrichment (ephemeral)
│   │   │   ├── int_trade_counterparties.sql
│   │   │   ├── int_vessel_risk_profile.sql
│   │   │   └── int_screening_enriched.sql
│   │   └── marts/
│   │       ├── core/                  # Gold - Facts & Dimensions
│   │       │   ├── dim_counterparties.sql
│   │       │   ├── dim_vessels.sql
│   │       │   ├── dim_sanctions_lists.sql
│   │       │   ├── fct_trades.sql
│   │       │   ├── fct_screening_results.sql
│   │       │   └── fct_vessel_movements.sql
│   │       └── derived/               # Gold - Aggregates
│   │           ├── agg_daily_trade_summary.sql
│   │           ├── agg_screening_performance.sql
│   │           └── agg_counterparty_exposure.sql
│   ├── seeds/                         # Reference data
│   │   ├── seed_country_codes.csv
│   │   ├── seed_commodity_codes.csv
│   │   └── seed_sanctions_programs.csv
│   └── snapshots/                     # SCD Type 2 tracking
│       ├── snap_counterparties.sql
│       ├── snap_sanctions_lists.sql
│       └── snap_vessels.sql
│
├── .github/workflows/                 # CI/CD pipelines
│   ├── dbt_ci.yml                     # DBT lint, build, test
│   ├── data_generator_ci.yml          # Python lint & test
│   └── snowflake_deploy.yml           # SQL deployment (manual trigger)
│
└── README.md                          # This file
```

## Prerequisites

- **Snowflake Account** with ACCOUNTADMIN or equivalent privileges
- **Python 3.11+** for data generation
- **dbt-snowflake 1.7+** for transformations
- **Git** for version control

## Quick Start

### 1. Snowflake Setup

Execute the SQL scripts in order against your Snowflake account:

```bash
# Using SnowSQL CLI
snowsql -a <account> -u <user> -f snowflake/001_account_setup.sql
snowsql -a <account> -u <user> -f snowflake/002_external_stages.sql
snowsql -a <account> -u <user> -f snowflake/003_raw_tables.sql
snowsql -a <account> -u <user> -f snowflake/004_snowpipe_setup.sql
snowsql -a <account> -u <user> -f snowflake/005_streaming_setup.sql
```

### 2. Generate Synthetic Data

```bash
cd sanctions_data_generator

# Create virtual environment
python -m venv .venv && source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Copy and configure environment
cp .env.example .env
# Edit .env with your Snowflake credentials

# Generate DEV data (~5K-50K records per entity)
python generate_all_data.py --env dev --output ./generated_data

# Generate TEST data (~50K-500K records per entity)
python generate_all_data.py --env test --output ./generated_data

# Generate PROD data (BILLIONS scale - use with caution)
python generate_all_data.py --env prod --output ./generated_data --workers 8
```

### 3. Load Data to Snowflake

```bash
# Load generated Parquet files via PUT + COPY INTO
python load_to_snowflake.py --manifest ./generated_data/manifest.json

# Verify row counts
python load_to_snowflake.py --manifest ./generated_data/manifest.json --verify-only
```

### 4. Run DBT Transformations

```bash
cd sanctions_dbt

# Install dbt packages
dbt deps

# Run seeds (reference data)
dbt seed

# Run snapshots (SCD Type 2)
dbt snapshot

# Build all models (staging → intermediate → marts)
dbt build

# Generate documentation
dbt docs generate && dbt docs serve
```

## Role-Based Access Control (RBAC)

| Role                          | Purpose                           | Access Level    |
|-------------------------------|-----------------------------------|-----------------|
| `SANCTIONS_PLATFORM_ADMIN`    | Platform administration           | Full            |
| `SANCTIONS_DATA_ENGINEER`     | Pipeline development & operations | Read/Write RAW  |
| `SANCTIONS_ANALYST`           | Business analysis & reporting     | Read ANALYTICS  |
| `SANCTIONS_DATA_SCIENTIST`    | ML model development              | Read/Write ML   |
| `SANCTIONS_COMPLIANCE_OFFICER`| Regulatory compliance             | Read ALL        |
| `SANCTIONS_DBT_RUNNER`        | DBT service account               | Transform       |
| `SANCTIONS_STREAMLIT_APP`     | Streamlit application             | Read ANALYTICS  |
| `SANCTIONS_ML_PIPELINE`       | ML pipeline service               | Read/Write ML   |

## Warehouse Strategy

| Warehouse                     | Size | Auto-Suspend | Purpose                      |
|-------------------------------|------|--------------|------------------------------|
| `SANCTIONS_LOADING_WH_XS`    | XS   | 60s          | Light ingestion              |
| `SANCTIONS_LOADING_WH_M`     | M    | 120s         | Heavy bulk loads             |
| `SANCTIONS_TRANSFORM_WH_XS`  | XS   | 60s          | DBT dev transforms           |
| `SANCTIONS_TRANSFORM_WH_M`   | M    | 120s         | DBT prod transforms          |
| `SANCTIONS_ANALYTICS_WH_XS`  | XS   | 60s          | Ad-hoc queries               |
| `SANCTIONS_ANALYTICS_WH_S`   | S    | 120s         | Dashboard queries             |
| `SANCTIONS_ML_WH_M`          | M    | 300s         | ML training & inference      |

## Key Design Decisions

1. **TRANSIENT tables** for raw layer — reduces storage costs and Fail-safe overhead
2. **Clustering keys** on high-volume tables (trades by date, movements by date+vessel)
3. **Search optimization** on critical lookup columns for sub-second screening
4. **Incremental models** for billion-row fact tables (append strategy)
5. **Snowpipe** for continuous ingestion with SQS/Event Grid notifications
6. **Dynamic Tables** for near-real-time risk scoring (2-5 minute lag)
7. **Streams + Tasks** for CDC processing of sanctions alerts
8. **SCD Type 2 snapshots** for audit trail on counterparties, vessels, sanctions lists
9. **Resource monitors** for cost control with tiered alerting
10. **Object tagging** for governance, cost attribution, and data classification

## Environment Configuration

| Parameter          | DEV                | TEST               | PROD               |
|--------------------|--------------------|--------------------|---------------------|
| Database           | `SANCTIONS_DEV`    | `SANCTIONS_TEST`   | `SANCTIONS_PROD`    |
| Data Retention     | 1 day              | 7 days             | 90 days             |
| Volume Multiplier  | 0.001x             | 0.01x              | 1.0x                |
| DBT Threads        | 4                  | 8                  | 16                  |
| Warehouse Size     | XS                 | XS                 | M                   |

## Contact

**Prism Data Labs** — info@prismdatalabs.co.uk
