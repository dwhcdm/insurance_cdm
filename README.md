# Commodity Trading Sanctions Risk Analytics Platform — Database Setup

> **Step 1 of N** — Snowflake infrastructure, data generation, and DBT transformation layer for an enterprise-grade sanctions risk analytics platform operating at billions-scale.

---

## Architecture Overview

```
┌───────────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                                       │
│  KYC Systems │ Sanctions Lists │ AIS Feed │ Trading Systems │ Screening  │
└──────┬───────────────┬──────────────┬──────────────┬──────────────┬───────┘
       │               │              │              │              │
       ▼               ▼              ▼              ▼              ▼
┌───────────────────────────────────────────────────────────────────────────┐
│                     INGESTION LAYER                                       │
│  Snowpipe (auto-ingest) │ Snowpipe Streaming │ PUT/COPY INTO (bulk)      │
└──────────────────────────────────┬────────────────────────────────────────┘
                                   │
                                   ▼
┌───────────────────────────────────────────────────────────────────────────┐
│                   SNOWFLAKE DATA WAREHOUSE                                │
│                                                                           │
│  ┌──────────┐  ┌───────────┐  ┌───────────┐  ┌──────┐  ┌──────────────┐ │
│  │   RAW    │→ │  STAGING  │→ │ ANALYTICS │→ │  ML  │→ │ CONSUMPTION  │ │
│  │ (Bronze) │  │ (Silver)  │  │  (Gold)   │  │      │  │              │ │
│  └──────────┘  └───────────┘  └───────────┘  └──────┘  └──────────────┘ │
│                                                                           │
│  CURATED │ DERIVED │ GOVERNANCE │ GENAI                                   │
└───────────────────────────────────────────────────────────────────────────┘
```

## Production Data Volumes

| Entity | Daily Volume | Annual Volume | Storage Estimate |
|---|---|---|---|
| Counterparties | — | 5,000,000 | ~10 GB |
| Sanctions Lists | — | 250,000 | ~500 MB |
| Vessels | — | 500,000 | ~2 GB |
| Trades | 2,500,000 | 912,500,000 | ~2 TB |
| Vessel Movements (AIS) | 10,000,000 | 3,650,000,000 | ~8 TB |
| Screening Results | 375,000 | 136,875,000 | ~500 GB |

**Total**: ~5 billion records/year, ~11 TB estimated storage.

## Project Structure

```
sanctions-risk-platform/
│
├── snowflake/                          # Snowflake SQL setup scripts
│   ├── 001_account_setup.sql           # Account, databases, schemas, warehouses, RBAC
│   ├── 002_external_stages.sql         # File formats, internal/external stages
│   ├── 003_raw_tables.sql              # Raw (Bronze) table definitions + tags
│   ├── 004_snowpipe_setup.sql          # Snowpipe definitions for automated ingestion
│   └── 005_streaming_setup.sql         # Streams, tasks, dynamic tables for CDC
│
├── sanctions_data_generator/           # Python synthetic data generation
│   ├── config.py                       # Configuration dataclasses
│   ├── generators/
│   │   ├── base_generator.py           # Abstract base with Parquet batch output
│   │   ├── counterparty_generator.py   # 5M counterparties
│   │   ├── sanctions_list_generator.py # 250K sanctioned entities
│   │   ├── vessel_generator.py         # 500K vessels
│   │   ├── vessel_movement_generator.py# 10M AIS records/day
│   │   ├── trade_generator.py          # 2.5M trades/day
│   │   └── screening_generator.py      # 375K screening results/day
│   ├── generate_all_data.py            # Orchestrator with environment scaling
│   ├── load_to_snowflake.py            # PUT + COPY INTO loader
│   ├── requirements.txt                # Python dependencies
│   └── .env.example                    # Environment variable template
│
├── sanctions_dbt/                      # DBT transformation project
│   ├── dbt_project.yml                 # Project configuration
│   ├── packages.yml                    # dbt-utils, dbt-expectations, etc.
│   ├── profiles.yml                    # Snowflake connection profiles (dev/test/prod)
│   ├── .sqlfluff                       # SQL linting configuration
│   ├── macros/
│   │   ├── generate_schema_name.sql    # Custom schema routing
│   │   ├── grant_select_on_schemas.sql # Automated grants post-hook
│   │   ├── audit_columns.sql           # _dbt_loaded_at, _dbt_invocation_id
│   │   ├── hash_surrogate_key.sql      # MD5-based surrogate key generation
│   │   └── test_macros/
│   │       ├── test_not_negative.sql   # Custom non-negative test
│   │       └── test_valid_country_code.sql
│   ├── models/
│   │   ├── staging/                    # Views — cleanse and type-cast raw data
│   │   │   ├── _staging__sources.yml   # Source definitions with freshness SLAs
│   │   │   ├── _staging__models.yml    # Model tests and documentation
│   │   │   ├── stg_counterparties.sql
│   │   │   ├── stg_sanctions_lists.sql
│   │   │   ├── stg_vessels.sql
│   │   │   ├── stg_trades.sql
│   │   │   ├── stg_vessel_movements.sql
│   │   │   └── stg_screening_results.sql
│   │   ├── intermediate/              # Ephemeral — join and enrich
│   │   │   ├── int_trade_counterparties.sql
│   │   │   ├── int_vessel_risk_profile.sql
│   │   │   └── int_screening_enriched.sql
│   │   └── marts/
│   │       ├── core/                  # Tables — dimensions and facts
│   │       │   ├── dim_counterparties.sql
│   │       │   ├── dim_vessels.sql
│   │       │   ├── dim_sanctions_lists.sql
│   │       │   ├── fct_trades.sql
│   │       │   ├── fct_screening_results.sql
│   │       │   └── fct_vessel_movements.sql
│   │       └── derived/               # Incremental — aggregates
│   │           ├── agg_daily_trade_summary.sql
│   │           ├── agg_screening_performance.sql
│   │           └── agg_counterparty_exposure.sql
│   ├── seeds/
│   │   ├── seed_country_codes.csv      # 78 countries with risk classification
│   │   ├── seed_commodity_codes.csv    # 44 commodities across 10 groups
│   │   └── seed_sanctions_programs.csv # 16 sanctions programs
│   └── snapshots/                     # SCD Type 2 history tracking
│       ├── snap_counterparties.sql
│       ├── snap_sanctions_lists.sql
│       └── snap_vessels.sql
│
├── .github/workflows/
│   ├── dbt_ci.yml                     # DBT lint + compile on PR
│   ├── data_generator_ci.yml          # Python lint + test on PR
│   └── snowflake_deploy.yml           # Manual SQL deployment trigger
│
├── .gitignore
└── README.md
```

## Setup Instructions

### Prerequisites

- Python 3.10+
- Snowflake account with ACCOUNTADMIN access (for initial setup)
- dbt-snowflake 1.7+

### 1. Snowflake Account Setup

Execute the SQL scripts in order against your Snowflake account:

```bash
# Using SnowSQL CLI
snowsql -a <account> -u <admin_user> -f snowflake/001_account_setup.sql
snowsql -a <account> -u <admin_user> -f snowflake/002_external_stages.sql
snowsql -a <account> -u <admin_user> -f snowflake/003_raw_tables.sql
snowsql -a <account> -u <admin_user> -f snowflake/004_snowpipe_setup.sql
snowsql -a <account> -u <admin_user> -f snowflake/005_streaming_setup.sql
```

### 2. Generate Synthetic Data

```bash
cd sanctions_data_generator
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
# Edit .env with your Snowflake credentials

# Generate dev-scale data (0.1% of production)
python generate_all_data.py --env dev

# Generate test-scale data (1% of production)
python generate_all_data.py --env test
```

### 3. Load Data into Snowflake

```bash
python load_to_snowflake.py --manifest generated_data/manifest.json
```

### 4. Run DBT Transformations

```bash
cd sanctions_dbt
pip install dbt-snowflake

# Set environment variables
export SNOWFLAKE_ACCOUNT=your_account
export SNOWFLAKE_USER=your_user
export SNOWFLAKE_PASSWORD=your_password

dbt deps
dbt seed --target dev
dbt snapshot --target dev
dbt run --target dev
dbt test --target dev
```

## RBAC Role Hierarchy

```
ACCOUNTADMIN
└── SANCTIONS_PLATFORM_ADMIN
    ├── SANCTIONS_DATA_ENGINEER     → Full DDL/DML on RAW, STAGING
    ├── SANCTIONS_DBT_RUNNER        → Service account for dbt runs
    ├── SANCTIONS_ANALYST           → SELECT on ANALYTICS, DERIVED
    ├── SANCTIONS_DATA_SCIENTIST    → SELECT + ML schema access
    ├── SANCTIONS_COMPLIANCE_OFFICER→ Full SELECT across all schemas
    ├── SANCTIONS_ML_PIPELINE       → Service account for ML workloads
    └── SANCTIONS_STREAMLIT_APP     → Service account for Streamlit apps
```

## Warehouse Strategy

| Warehouse | Size | Purpose | Auto-Suspend |
|---|---|---|---|
| SANCTIONS_LOADING_WH_XS | X-Small | Light bulk loads, metadata queries | 60s |
| SANCTIONS_LOADING_WH_M | Medium | Heavy bulk data loading | 120s |
| SANCTIONS_TRANSFORM_WH_XS | X-Small | dbt dev runs, ad-hoc transforms | 60s |
| SANCTIONS_TRANSFORM_WH_M | Medium | dbt production runs | 120s |
| SANCTIONS_ANALYTICS_WH_XS | X-Small | Light analytics queries | 60s |
| SANCTIONS_ANALYTICS_WH_S | Small | Dashboard queries, BI tools | 120s |
| SANCTIONS_ML_WH_M | Medium | ML training and inference | 300s |

## Key Design Decisions

1. **TRANSIENT tables for RAW layer** — Reduces Time Travel storage costs for high-volume landing tables that are reloadable from source.
2. **Clustered tables** — `RAW_VESSEL_MOVEMENTS` clustered on `(timestamp, vessel_id)` for time-range + vessel lookups. Trade tables clustered on `(trade_date, commodity_group)`.
3. **Search Optimization** — Enabled on `RAW_VESSEL_MOVEMENTS` for point lookups on `movement_id` and `vessel_id`.
4. **Incremental models for high-volume facts** — `fct_vessel_movements` uses append strategy; derived aggregates use merge strategy.
5. **Ephemeral intermediate models** — `int_*` models are compiled inline to avoid materialising intermediate results for billion-row joins.
6. **SCD Type 2 snapshots** — Track historical changes to counterparties, vessels, and sanctions lists for regulatory audit trail.
7. **Dynamic Tables** — `DT_TRADE_RISK_REALTIME` (5-min lag) and `DT_VESSEL_DARK_ACTIVITY` (2-min lag) for near-real-time risk dashboards.
8. **Environment-scaled data generation** — Dev (0.1%), Test (1%), Prod (100%) multipliers for realistic testing without full-scale costs.

## Next Steps

This is Step 1 (Database Setup). Subsequent steps will cover:
- Streamlit dashboards and applications
- ML model training and deployment
- GenAI integration for sanctions research
- Monitoring and alerting infrastructure
- Production deployment and operations runbooks
