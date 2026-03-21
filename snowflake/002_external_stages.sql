-- ============================================================================
-- 002_external_stages.sql
-- File formats, internal stages, and external stage templates
-- ============================================================================
-- EXECUTION ORDER: Run AFTER 001_account_setup.sql
-- ============================================================================

USE ROLE SANCTIONS_DATA_ENGINEER;
USE DATABASE SANCTIONS_DEV;
USE SCHEMA RAW;

-- ============================================================================
-- SECTION 1: FILE FORMATS
-- ============================================================================

-- Standard CSV (comma-delimited, header row)
CREATE OR REPLACE FILE FORMAT FF_CSV_STANDARD
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    TRIM_SPACE = TRUE
    NULL_IF = ('NULL', 'null', '', '\\N')
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    ENCODING = 'UTF8'
    COMMENT = 'Standard CSV format with header row';

-- Pipe-delimited CSV
CREATE OR REPLACE FILE FORMAT FF_CSV_PIPE
    TYPE = 'CSV'
    FIELD_DELIMITER = '|'
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    TRIM_SPACE = TRUE
    NULL_IF = ('NULL', 'null', '', '\\N')
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    ENCODING = 'UTF8'
    COMMENT = 'Pipe-delimited CSV format';

-- JSON (standard)
CREATE OR REPLACE FILE FORMAT FF_JSON
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = TRUE
    STRIP_NULL_VALUES = FALSE
    IGNORE_UTF8_ERRORS = FALSE
    COMMENT = 'Standard JSON format with outer array stripping';

-- JSON (raw - preserve structure)
CREATE OR REPLACE FILE FORMAT FF_JSON_RAW
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = FALSE
    STRIP_NULL_VALUES = FALSE
    COMMENT = 'Raw JSON format preserving full structure';

-- Parquet (primary format for bulk loading)
CREATE OR REPLACE FILE FORMAT FF_PARQUET
    TYPE = 'PARQUET'
    SNAPPY_COMPRESSION = TRUE
    BINARY_AS_TEXT = FALSE
    COMMENT = 'Parquet format with Snappy compression for bulk loading';

-- ============================================================================
-- SECTION 2: INTERNAL STAGES
-- ============================================================================

-- Trade data stage
CREATE OR REPLACE STAGE STG_TRADE_DATA
    FILE_FORMAT = FF_PARQUET
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Internal stage for trade transaction data files';

-- Sanctions lists stage
CREATE OR REPLACE STAGE STG_SANCTIONS_LISTS
    FILE_FORMAT = FF_PARQUET
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Internal stage for sanctions list data files';

-- Vessel data stage (movements + master)
CREATE OR REPLACE STAGE STG_VESSEL_DATA
    FILE_FORMAT = FF_PARQUET
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Internal stage for vessel master and AIS movement data';

-- Counterparty / KYC data stage
CREATE OR REPLACE STAGE STG_COUNTERPARTY_DATA
    FILE_FORMAT = FF_PARQUET
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Internal stage for counterparty and KYC data files';

-- Screening results stage
CREATE OR REPLACE STAGE STG_SCREENING_DATA
    FILE_FORMAT = FF_PARQUET
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Internal stage for screening result data files';

-- ML artifacts stage
CREATE OR REPLACE STAGE STG_ML_ARTIFACTS
    FILE_FORMAT = FF_PARQUET
    DIRECTORY = (ENABLE = TRUE)
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
    COMMENT = 'Internal stage for ML model artifacts and feature files';

-- Streamlit files stage
CREATE OR REPLACE STAGE STG_STREAMLIT_FILES
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Internal stage for Streamlit application files';

-- ============================================================================
-- SECTION 3: EXTERNAL STAGE TEMPLATES
-- ============================================================================
-- Uncomment and configure based on your cloud provider.

-- ── AWS S3 ──
-- CREATE OR REPLACE STAGE STG_S3_TRADE_DATA
--     URL = 's3://your-bucket/sanctions-platform/trades/'
--     STORAGE_INTEGRATION = SANCTIONS_S3_INTEGRATION
--     FILE_FORMAT = FF_PARQUET
--     DIRECTORY = (ENABLE = TRUE)
--     COMMENT = 'External S3 stage for trade data';

-- CREATE OR REPLACE STORAGE INTEGRATION SANCTIONS_S3_INTEGRATION
--     TYPE = EXTERNAL_STAGE
--     STORAGE_PROVIDER = 'S3'
--     ENABLED = TRUE
--     STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::ACCOUNT_ID:role/sanctions-snowflake-role'
--     STORAGE_ALLOWED_LOCATIONS = (
--         's3://your-bucket/sanctions-platform/'
--     );

-- ── Azure Blob Storage ──
-- CREATE OR REPLACE STAGE STG_AZURE_TRADE_DATA
--     URL = 'azure://your-account.blob.core.windows.net/sanctions-platform/trades/'
--     STORAGE_INTEGRATION = SANCTIONS_AZURE_INTEGRATION
--     FILE_FORMAT = FF_PARQUET
--     DIRECTORY = (ENABLE = TRUE)
--     COMMENT = 'External Azure stage for trade data';

-- CREATE OR REPLACE STORAGE INTEGRATION SANCTIONS_AZURE_INTEGRATION
--     TYPE = EXTERNAL_STAGE
--     STORAGE_PROVIDER = 'AZURE'
--     ENABLED = TRUE
--     AZURE_TENANT_ID = 'your-tenant-id'
--     STORAGE_ALLOWED_LOCATIONS = (
--         'azure://your-account.blob.core.windows.net/sanctions-platform/'
--     );

-- ── Google Cloud Storage ──
-- CREATE OR REPLACE STAGE STG_GCS_TRADE_DATA
--     URL = 'gcs://your-bucket/sanctions-platform/trades/'
--     STORAGE_INTEGRATION = SANCTIONS_GCS_INTEGRATION
--     FILE_FORMAT = FF_PARQUET
--     DIRECTORY = (ENABLE = TRUE)
--     COMMENT = 'External GCS stage for trade data';

-- CREATE OR REPLACE STORAGE INTEGRATION SANCTIONS_GCS_INTEGRATION
--     TYPE = EXTERNAL_STAGE
--     STORAGE_PROVIDER = 'GCS'
--     ENABLED = TRUE
--     STORAGE_ALLOWED_LOCATIONS = (
--         'gcs://your-bucket/sanctions-platform/'
--     );

-- End of 002_external_stages.sql
