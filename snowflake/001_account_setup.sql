-- ============================================================================
-- SCRIPT: 001_account_setup.sql
-- PURPOSE: Enterprise account configuration for Commodity Trading Sanctions
--          Risk Analytics Platform
-- RUN AS:  ACCOUNTADMIN
-- VERSION: 1.0.0
-- ============================================================================

-- 0. Set session context
USE ROLE ACCOUNTADMIN;

-- ============================================================================
-- 1. ACCOUNT-LEVEL PARAMETERS (Enterprise Best Practices)
-- ============================================================================

ALTER ACCOUNT SET
    PERIODIC_DATA_REKEYING = TRUE                    -- Auto re-encryption
    NETWORK_POLICY = ''                              -- Will set after creation
    MIN_DATA_RETENTION_TIME_IN_DAYS = 1              -- Minimum retention
    ENABLE_ACCOUNT_DATABASE_REPLICATION = TRUE;

-- Enable Cortex AI features (required for GenAI modules)
ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';

-- Enable Snowpark and ML features
ALTER ACCOUNT SET ENABLE_UNREDACTED_QUERY_SYNTAX_ERROR = TRUE;

-- ============================================================================
-- 2. DATABASE STRUCTURE
-- ============================================================================

-- Create environment databases
CREATE DATABASE IF NOT EXISTS SANCTIONS_DEV
    DATA_RETENTION_TIME_IN_DAYS = 1
    COMMENT = 'Development environment for Sanctions Risk Platform';

CREATE DATABASE IF NOT EXISTS SANCTIONS_TEST
    DATA_RETENTION_TIME_IN_DAYS = 1
    COMMENT = 'Test/QA environment for Sanctions Risk Platform';

CREATE DATABASE IF NOT EXISTS SANCTIONS_PROD
    DATA_RETENTION_TIME_IN_DAYS = 90
    COMMENT = 'Production environment for Sanctions Risk Platform';

-- ============================================================================
-- 3. SCHEMA STRUCTURE (Consistent across all environments)
-- ============================================================================

-- Using a stored procedure to create consistent schema structure
CREATE OR REPLACE PROCEDURE ACCOUNTADMIN.PUBLIC.CREATE_SCHEMA_STRUCTURE(DB_NAME VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    -- Raw/Landing Layer
    EXECUTE IMMEDIATE 'CREATE SCHEMA IF NOT EXISTS ' || DB_NAME || '.RAW
        WITH MANAGED ACCESS
        DATA_RETENTION_TIME_IN_DAYS = 1
        COMMENT = ''Raw data landing zone - Bronze layer''';

    -- Staging Layer
    EXECUTE IMMEDIATE 'CREATE SCHEMA IF NOT EXISTS ' || DB_NAME || '.STAGING
        WITH MANAGED ACCESS
        DATA_RETENTION_TIME_IN_DAYS = 1
        COMMENT = ''Staging/transformation layer''';

    -- Curated/Silver Layer
    EXECUTE IMMEDIATE 'CREATE SCHEMA IF NOT EXISTS ' || DB_NAME || '.CURATED
        WITH MANAGED ACCESS
        DATA_RETENTION_TIME_IN_DAYS = 7
        COMMENT = ''Curated layer - Silver''';

    -- Analytics/Gold Layer
    EXECUTE IMMEDIATE 'CREATE SCHEMA IF NOT EXISTS ' || DB_NAME || '.ANALYTICS
        WITH MANAGED ACCESS
        DATA_RETENTION_TIME_IN_DAYS = 90
        COMMENT = ''Analytics layer - Gold - Facts and Dimensions''';

    -- Derived/Aggregates Layer
    EXECUTE IMMEDIATE 'CREATE SCHEMA IF NOT EXISTS ' || DB_NAME || '.DERIVED
        WITH MANAGED ACCESS
        COMMENT = ''Derived tables and aggregates''';

    -- ML Layer
    EXECUTE IMMEDIATE 'CREATE SCHEMA IF NOT EXISTS ' || DB_NAME || '.ML
        WITH MANAGED ACCESS
        COMMENT = ''Machine Learning models and features''';

    -- GenAI Layer
    EXECUTE IMMEDIATE 'CREATE SCHEMA IF NOT EXISTS ' || DB_NAME || '.GENAI
        WITH MANAGED ACCESS
        COMMENT = ''Generative AI and Cortex objects''';

    -- Governance Layer
    EXECUTE IMMEDIATE 'CREATE SCHEMA IF NOT EXISTS ' || DB_NAME || '.GOVERNANCE
        WITH MANAGED ACCESS
        COMMENT = ''Governance, audit, and metadata''';

    -- Consumption Layer
    EXECUTE IMMEDIATE 'CREATE SCHEMA IF NOT EXISTS ' || DB_NAME || '.CONSUMPTION
        WITH MANAGED ACCESS
        COMMENT = ''Views and objects for end-user consumption''';

    RETURN 'Schema structure created for ' || DB_NAME;
END;
$$;

-- Execute for all environments
CALL ACCOUNTADMIN.PUBLIC.CREATE_SCHEMA_STRUCTURE('SANCTIONS_DEV');
CALL ACCOUNTADMIN.PUBLIC.CREATE_SCHEMA_STRUCTURE('SANCTIONS_TEST');
CALL ACCOUNTADMIN.PUBLIC.CREATE_SCHEMA_STRUCTURE('SANCTIONS_PROD');

-- ============================================================================
-- 4. WAREHOUSE STRATEGY (Critical for Cost Optimization)
-- ============================================================================

-- Warehouse Taxonomy:
--   LOADING_WH:    For data ingestion (can be larger, auto-suspend aggressive)
--   TRANSFORM_WH:  For DBT/ELT transformations
--   ANALYTICS_WH:  For BI/reporting queries
--   ML_WH:         For Snowpark ML workloads (may need larger)
--   ADMIN_WH:      For administrative tasks

-- Loading Warehouse - XS for dev, scales for prod
CREATE WAREHOUSE IF NOT EXISTS SANCTIONS_LOADING_WH_XS
    WAREHOUSE_SIZE = 'X-SMALL'
    WAREHOUSE_TYPE = 'STANDARD'
    AUTO_SUSPEND = 60                  -- 1 minute (aggressive)
    AUTO_RESUME = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 1
    SCALING_POLICY = 'STANDARD'
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Data loading warehouse - Dev/Test';

CREATE WAREHOUSE IF NOT EXISTS SANCTIONS_LOADING_WH_M
    WAREHOUSE_SIZE = 'MEDIUM'
    WAREHOUSE_TYPE = 'STANDARD'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 4              -- Multi-cluster for parallel loads
    SCALING_POLICY = 'STANDARD'
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Data loading warehouse - Production bulk loads';

-- Transform Warehouse
CREATE WAREHOUSE IF NOT EXISTS SANCTIONS_TRANSFORM_WH_XS
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 120                 -- 2 minutes
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'DBT transformation warehouse - Dev';

CREATE WAREHOUSE IF NOT EXISTS SANCTIONS_TRANSFORM_WH_M
    WAREHOUSE_SIZE = 'MEDIUM'
    AUTO_SUSPEND = 120
    AUTO_RESUME = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 2
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'DBT transformation warehouse - Production';

-- Analytics Warehouse (BI queries)
CREATE WAREHOUSE IF NOT EXISTS SANCTIONS_ANALYTICS_WH_XS
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 300                 -- 5 minutes (users may run multiple queries)
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Analytics/BI warehouse - Dev';

CREATE WAREHOUSE IF NOT EXISTS SANCTIONS_ANALYTICS_WH_S
    WAREHOUSE_SIZE = 'SMALL'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 3
    SCALING_POLICY = 'ECONOMY'         -- Cost-optimized scaling
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Analytics/BI warehouse - Production';

-- ML Warehouse (Snowpark workloads)
CREATE WAREHOUSE IF NOT EXISTS SANCTIONS_ML_WH_M
    WAREHOUSE_SIZE = 'MEDIUM'
    AUTO_SUSPEND = 120
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Machine Learning warehouse';

-- Snowpark Optimized Warehouse for heavy ML (optional, higher cost)
CREATE WAREHOUSE IF NOT EXISTS SANCTIONS_ML_WH_SNOWPARK_M
    WAREHOUSE_SIZE = 'MEDIUM'
    WAREHOUSE_TYPE = 'SNOWPARK-OPTIMIZED'
    AUTO_SUSPEND = 120
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Snowpark-optimized warehouse for ML training';

-- Admin Warehouse (lightweight)
CREATE WAREHOUSE IF NOT EXISTS SANCTIONS_ADMIN_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Administrative tasks warehouse';

-- ============================================================================
-- 5. RESOURCE MONITORS (Cost Control)
-- ============================================================================

-- Overall account monitor
CREATE OR REPLACE RESOURCE MONITOR ACCOUNT_MONTHLY_MONITOR
    WITH CREDIT_QUOTA = 10000          -- Adjust based on budget
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS
        ON 50 PERCENT DO NOTIFY
        ON 75 PERCENT DO NOTIFY
        ON 90 PERCENT DO NOTIFY
        ON 100 PERCENT DO SUSPEND
        ON 110 PERCENT DO SUSPEND_IMMEDIATE;

ALTER ACCOUNT SET RESOURCE_MONITOR = ACCOUNT_MONTHLY_MONITOR;

-- Development environment monitor (strict)
CREATE OR REPLACE RESOURCE MONITOR DEV_DAILY_MONITOR
    WITH CREDIT_QUOTA = 50
    FREQUENCY = DAILY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS
        ON 75 PERCENT DO NOTIFY
        ON 100 PERCENT DO SUSPEND;

-- Production environment monitor
CREATE OR REPLACE RESOURCE MONITOR PROD_DAILY_MONITOR
    WITH CREDIT_QUOTA = 500
    FREQUENCY = DAILY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS
        ON 50 PERCENT DO NOTIFY
        ON 80 PERCENT DO NOTIFY
        ON 100 PERCENT DO NOTIFY         -- Notify only, don't suspend prod
        ON 120 PERCENT DO SUSPEND;

-- Assign monitors to warehouses
ALTER WAREHOUSE SANCTIONS_LOADING_WH_XS SET RESOURCE_MONITOR = DEV_DAILY_MONITOR;
ALTER WAREHOUSE SANCTIONS_TRANSFORM_WH_XS SET RESOURCE_MONITOR = DEV_DAILY_MONITOR;
ALTER WAREHOUSE SANCTIONS_ANALYTICS_WH_XS SET RESOURCE_MONITOR = DEV_DAILY_MONITOR;

ALTER WAREHOUSE SANCTIONS_LOADING_WH_M SET RESOURCE_MONITOR = PROD_DAILY_MONITOR;
ALTER WAREHOUSE SANCTIONS_TRANSFORM_WH_M SET RESOURCE_MONITOR = PROD_DAILY_MONITOR;
ALTER WAREHOUSE SANCTIONS_ANALYTICS_WH_S SET RESOURCE_MONITOR = PROD_DAILY_MONITOR;

-- ============================================================================
-- 6. ROLE HIERARCHY (Enterprise RBAC)
-- ============================================================================

-- Top-level platform admin
CREATE ROLE IF NOT EXISTS SANCTIONS_PLATFORM_ADMIN
    COMMENT = 'Platform administrator - full access';

-- Data engineering roles
CREATE ROLE IF NOT EXISTS SANCTIONS_DATA_ENGINEER
    COMMENT = 'Data engineers - ETL/ELT development';
CREATE ROLE IF NOT EXISTS SANCTIONS_DATA_ENGINEER_LEAD
    COMMENT = 'Senior data engineers - elevated privileges';

-- Analytics roles
CREATE ROLE IF NOT EXISTS SANCTIONS_ANALYST
    COMMENT = 'Business analysts - read access to analytics layer';
CREATE ROLE IF NOT EXISTS SANCTIONS_ANALYST_SENIOR
    COMMENT = 'Senior analysts - can create views/reports';

-- Data science roles
CREATE ROLE IF NOT EXISTS SANCTIONS_DATA_SCIENTIST
    COMMENT = 'Data scientists - ML model development';

-- Compliance roles (sensitive - sanctions data)
CREATE ROLE IF NOT EXISTS SANCTIONS_COMPLIANCE_OFFICER
    COMMENT = 'Compliance officers - full read access, audit capabilities';
CREATE ROLE IF NOT EXISTS SANCTIONS_COMPLIANCE_ADMIN
    COMMENT = 'Compliance admin - can manage screening rules';

-- Service account roles
CREATE ROLE IF NOT EXISTS SANCTIONS_DBT_RUNNER
    COMMENT = 'DBT CI/CD service account';
CREATE ROLE IF NOT EXISTS SANCTIONS_STREAMLIT_APP
    COMMENT = 'Streamlit application service account';
CREATE ROLE IF NOT EXISTS SANCTIONS_ML_PIPELINE
    COMMENT = 'ML pipeline service account';

-- Role hierarchy
GRANT ROLE SANCTIONS_DATA_ENGINEER TO ROLE SANCTIONS_DATA_ENGINEER_LEAD;
GRANT ROLE SANCTIONS_ANALYST TO ROLE SANCTIONS_ANALYST_SENIOR;
GRANT ROLE SANCTIONS_ANALYST_SENIOR TO ROLE SANCTIONS_COMPLIANCE_OFFICER;
GRANT ROLE SANCTIONS_DATA_ENGINEER_LEAD TO ROLE SANCTIONS_PLATFORM_ADMIN;
GRANT ROLE SANCTIONS_COMPLIANCE_ADMIN TO ROLE SANCTIONS_PLATFORM_ADMIN;
GRANT ROLE SANCTIONS_DATA_SCIENTIST TO ROLE SANCTIONS_PLATFORM_ADMIN;
GRANT ROLE SANCTIONS_PLATFORM_ADMIN TO ROLE SYSADMIN;

-- Service accounts report to platform admin
GRANT ROLE SANCTIONS_DBT_RUNNER TO ROLE SANCTIONS_PLATFORM_ADMIN;
GRANT ROLE SANCTIONS_STREAMLIT_APP TO ROLE SANCTIONS_PLATFORM_ADMIN;
GRANT ROLE SANCTIONS_ML_PIPELINE TO ROLE SANCTIONS_PLATFORM_ADMIN;

-- ============================================================================
-- 7. GRANT PRIVILEGES
-- ============================================================================

-- Database grants --

-- Platform Admin - full access
GRANT ALL PRIVILEGES ON DATABASE SANCTIONS_DEV TO ROLE SANCTIONS_PLATFORM_ADMIN;
GRANT ALL PRIVILEGES ON DATABASE SANCTIONS_TEST TO ROLE SANCTIONS_PLATFORM_ADMIN;
GRANT ALL PRIVILEGES ON DATABASE SANCTIONS_PROD TO ROLE SANCTIONS_PLATFORM_ADMIN;

-- Data Engineers - DEV full, TEST/PROD limited
GRANT ALL PRIVILEGES ON DATABASE SANCTIONS_DEV TO ROLE SANCTIONS_DATA_ENGINEER;
GRANT USAGE ON DATABASE SANCTIONS_TEST TO ROLE SANCTIONS_DATA_ENGINEER;
GRANT USAGE ON DATABASE SANCTIONS_PROD TO ROLE SANCTIONS_DATA_ENGINEER;

-- Analysts - read access to analytics layer
GRANT USAGE ON DATABASE SANCTIONS_DEV TO ROLE SANCTIONS_ANALYST;
GRANT USAGE ON DATABASE SANCTIONS_PROD TO ROLE SANCTIONS_ANALYST;

-- Schema grants (using future grants for automation) --

-- Data Engineer grants on DEV
GRANT ALL PRIVILEGES ON ALL SCHEMAS IN DATABASE SANCTIONS_DEV TO ROLE SANCTIONS_DATA_ENGINEER;
GRANT ALL PRIVILEGES ON FUTURE SCHEMAS IN DATABASE SANCTIONS_DEV TO ROLE SANCTIONS_DATA_ENGINEER;

-- Analyst grants (analytics and consumption only)
GRANT USAGE ON SCHEMA SANCTIONS_DEV.ANALYTICS TO ROLE SANCTIONS_ANALYST;
GRANT USAGE ON SCHEMA SANCTIONS_DEV.CONSUMPTION TO ROLE SANCTIONS_ANALYST;
GRANT USAGE ON SCHEMA SANCTIONS_DEV.DERIVED TO ROLE SANCTIONS_ANALYST;
GRANT USAGE ON SCHEMA SANCTIONS_PROD.ANALYTICS TO ROLE SANCTIONS_ANALYST;
GRANT USAGE ON SCHEMA SANCTIONS_PROD.CONSUMPTION TO ROLE SANCTIONS_ANALYST;
GRANT USAGE ON SCHEMA SANCTIONS_PROD.DERIVED TO ROLE SANCTIONS_ANALYST;

-- Future table grants
GRANT SELECT ON ALL TABLES IN SCHEMA SANCTIONS_DEV.ANALYTICS TO ROLE SANCTIONS_ANALYST;
GRANT SELECT ON FUTURE TABLES IN SCHEMA SANCTIONS_DEV.ANALYTICS TO ROLE SANCTIONS_ANALYST;
GRANT SELECT ON ALL VIEWS IN SCHEMA SANCTIONS_DEV.ANALYTICS TO ROLE SANCTIONS_ANALYST;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA SANCTIONS_DEV.ANALYTICS TO ROLE SANCTIONS_ANALYST;

-- Warehouse grants --
GRANT USAGE ON WAREHOUSE SANCTIONS_LOADING_WH_XS TO ROLE SANCTIONS_DATA_ENGINEER;
GRANT USAGE ON WAREHOUSE SANCTIONS_TRANSFORM_WH_XS TO ROLE SANCTIONS_DATA_ENGINEER;
GRANT USAGE ON WAREHOUSE SANCTIONS_ANALYTICS_WH_XS TO ROLE SANCTIONS_ANALYST;
GRANT USAGE ON WAREHOUSE SANCTIONS_ML_WH_M TO ROLE SANCTIONS_DATA_SCIENTIST;

-- DBT Runner needs transform warehouse
GRANT USAGE ON WAREHOUSE SANCTIONS_TRANSFORM_WH_XS TO ROLE SANCTIONS_DBT_RUNNER;
GRANT USAGE ON WAREHOUSE SANCTIONS_TRANSFORM_WH_M TO ROLE SANCTIONS_DBT_RUNNER;

-- ============================================================================
-- 8. NETWORK POLICY (Enterprise Security)
-- ============================================================================

-- Create network policy (adjust IPs for your organization)
CREATE OR REPLACE NETWORK POLICY SANCTIONS_NETWORK_POLICY
    ALLOWED_IP_LIST = (
        '0.0.0.0/0'                    -- REPLACE with actual corporate IPs
        -- '10.0.0.0/8'               -- Internal network
        -- '192.168.0.0/16'           -- VPN range
        -- '203.0.113.0/24'           -- Office IP range
    )
    BLOCKED_IP_LIST = ()
    COMMENT = 'Enterprise network policy - restrict to corporate IPs';

-- Apply network policy (uncomment when ready)
-- ALTER ACCOUNT SET NETWORK_POLICY = SANCTIONS_NETWORK_POLICY;

-- ============================================================================
-- 9. OBJECT TAGGING (Governance & Cost Attribution)
-- ============================================================================

-- Create tag database and tags
CREATE DATABASE IF NOT EXISTS GOVERNANCE
    COMMENT = 'Central governance database for tags, policies, and metadata';
CREATE SCHEMA IF NOT EXISTS GOVERNANCE.TAGS;
CREATE SCHEMA IF NOT EXISTS GOVERNANCE.PUBLIC;

-- Cost attribution tags
CREATE OR REPLACE TAG GOVERNANCE.TAGS.COST_CENTER
    ALLOWED_VALUES = ('COMPLIANCE', 'TRADING', 'RISK', 'IT', 'DATA_SCIENCE')
    COMMENT = 'Cost center for chargeback';

CREATE OR REPLACE TAG GOVERNANCE.TAGS.ENVIRONMENT
    ALLOWED_VALUES = ('DEV', 'TEST', 'PROD')
    COMMENT = 'Environment classification';

CREATE OR REPLACE TAG GOVERNANCE.TAGS.DATA_DOMAIN
    ALLOWED_VALUES = ('TRADE', 'COUNTERPARTY', 'SANCTIONS', 'VESSEL', 'GEOGRAPHY', 'SCREENING')
    COMMENT = 'Business data domain';

-- Data classification tags (for security)
CREATE OR REPLACE TAG GOVERNANCE.TAGS.DATA_CLASSIFICATION
    ALLOWED_VALUES = ('PUBLIC', 'INTERNAL', 'CONFIDENTIAL', 'RESTRICTED', 'PII', 'SANCTIONS_SENSITIVE')
    COMMENT = 'Data classification level';

CREATE OR REPLACE TAG GOVERNANCE.TAGS.PII_TYPE
    ALLOWED_VALUES = ('NONE', 'NAME', 'ADDRESS', 'ID_NUMBER', 'FINANCIAL', 'CONTACT')
    COMMENT = 'Type of PII data';

CREATE OR REPLACE TAG GOVERNANCE.TAGS.RETENTION_DAYS
    COMMENT = 'Data retention period in days';

-- Apply tags to databases
ALTER DATABASE SANCTIONS_DEV SET TAG
    GOVERNANCE.TAGS.ENVIRONMENT = 'DEV',
    GOVERNANCE.TAGS.COST_CENTER = 'IT';
ALTER DATABASE SANCTIONS_TEST SET TAG
    GOVERNANCE.TAGS.ENVIRONMENT = 'TEST',
    GOVERNANCE.TAGS.COST_CENTER = 'IT';
ALTER DATABASE SANCTIONS_PROD SET TAG
    GOVERNANCE.TAGS.ENVIRONMENT = 'PROD',
    GOVERNANCE.TAGS.COST_CENTER = 'COMPLIANCE';

-- Apply tags to warehouses
ALTER WAREHOUSE SANCTIONS_LOADING_WH_XS SET TAG GOVERNANCE.TAGS.COST_CENTER = 'IT';
ALTER WAREHOUSE SANCTIONS_TRANSFORM_WH_XS SET TAG GOVERNANCE.TAGS.COST_CENTER = 'IT';
ALTER WAREHOUSE SANCTIONS_ANALYTICS_WH_XS SET TAG GOVERNANCE.TAGS.COST_CENTER = 'COMPLIANCE';
ALTER WAREHOUSE SANCTIONS_ML_WH_M SET TAG GOVERNANCE.TAGS.COST_CENTER = 'DATA_SCIENCE';

-- ============================================================================
-- 10. SERVICE ACCOUNTS (for CI/CD and Applications)
-- ============================================================================

-- Create service account users
CREATE USER IF NOT EXISTS SVC_DBT_RUNNER
    PASSWORD = 'CHANGE_ME_IMMEDIATELY'       -- Use key-pair auth in production
    DEFAULT_ROLE = SANCTIONS_DBT_RUNNER
    DEFAULT_WAREHOUSE = SANCTIONS_TRANSFORM_WH_XS
    MUST_CHANGE_PASSWORD = FALSE
    COMMENT = 'DBT CI/CD service account';

CREATE USER IF NOT EXISTS SVC_STREAMLIT_APP
    PASSWORD = 'CHANGE_ME_IMMEDIATELY'
    DEFAULT_ROLE = SANCTIONS_STREAMLIT_APP
    DEFAULT_WAREHOUSE = SANCTIONS_ANALYTICS_WH_XS
    MUST_CHANGE_PASSWORD = FALSE
    COMMENT = 'Streamlit application service account';

CREATE USER IF NOT EXISTS SVC_ML_PIPELINE
    PASSWORD = 'CHANGE_ME_IMMEDIATELY'
    DEFAULT_ROLE = SANCTIONS_ML_PIPELINE
    DEFAULT_WAREHOUSE = SANCTIONS_ML_WH_M
    MUST_CHANGE_PASSWORD = FALSE
    COMMENT = 'ML pipeline service account';

-- Grant roles to service accounts
GRANT ROLE SANCTIONS_DBT_RUNNER TO USER SVC_DBT_RUNNER;
GRANT ROLE SANCTIONS_STREAMLIT_APP TO USER SVC_STREAMLIT_APP;
GRANT ROLE SANCTIONS_ML_PIPELINE TO USER SVC_ML_PIPELINE;

-- ============================================================================
-- 11. KEY PAIR AUTHENTICATION (Production Best Practice)
-- ============================================================================

-- Instructions for key-pair setup (run locally, not in Snowflake):
/*
    Generate RSA key pair locally:

    openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
    openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub

    Then set the public key in Snowflake:
    ALTER USER SVC_DBT_RUNNER SET RSA_PUBLIC_KEY='MIIBIjANBgkqh...';
    ALTER USER SVC_DBT_RUNNER UNSET PASSWORD;
*/

-- ============================================================================
-- 12. QUERY TAG FOR COST ATTRIBUTION
-- ============================================================================

-- Create stored procedure for setting query tags
CREATE OR REPLACE PROCEDURE GOVERNANCE.PUBLIC.SET_QUERY_CONTEXT(
    COST_CENTER VARCHAR,
    APPLICATION VARCHAR,
    TEAM VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    LET query_tag := '{"cost_center":"' || COST_CENTER || '","application":"' || APPLICATION || '","team":"' || TEAM || '"}';
    EXECUTE IMMEDIATE 'ALTER SESSION SET QUERY_TAG = ''' || query_tag || '''';
    RETURN 'Query context set: ' || query_tag;
END;
$$;

-- Example usage:
-- CALL GOVERNANCE.PUBLIC.SET_QUERY_CONTEXT('COMPLIANCE', 'SANCTIONS_SCREENING', 'RISK_TEAM');

-- ============================================================================
-- 13. VERIFICATION QUERIES
-- ============================================================================

SHOW DATABASES LIKE 'SANCTIONS%';
SHOW SCHEMAS IN DATABASE SANCTIONS_DEV;
SHOW WAREHOUSES LIKE 'SANCTIONS%';
SHOW ROLES LIKE 'SANCTIONS%';
SHOW RESOURCE MONITORS;
SELECT SYSTEM$SHOW_ACTIVE_BEHAVIOR_CHANGE_BUNDLES();
