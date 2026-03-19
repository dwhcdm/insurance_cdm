-- ============================================================================
-- SCRIPT: 002_external_stages.sql
-- PURPOSE: Configure internal stages, file formats, and external stage
--          templates for data ingestion
-- RUN AS:  SANCTIONS_PLATFORM_ADMIN
-- VERSION: 1.0.0
-- ============================================================================

USE ROLE SANCTIONS_PLATFORM_ADMIN;
USE DATABASE SANCTIONS_DEV;
USE WAREHOUSE SANCTIONS_ADMIN_WH;

-- ============================================================================
-- 1. FILE FORMATS (Optimized for High-Volume Ingestion)
-- ============================================================================

-- Standard CSV (comma-delimited)
CREATE OR REPLACE FILE FORMAT RAW.FF_CSV_STANDARD
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    TRIM_SPACE = TRUE
    NULL_IF = ('NULL', 'null', '', '\\N', 'None')
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    COMPRESSION = 'AUTO'
    COMMENT = 'Standard CSV format with header row';

-- Pipe-delimited CSV (for data with embedded commas)
CREATE OR REPLACE FILE FORMAT RAW.FF_CSV_PIPE
    TYPE = 'CSV'
    FIELD_DELIMITER = '|'
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    TRIM_SPACE = TRUE
    NULL_IF = ('NULL', 'null', '', '\\N', 'None')
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    COMPRESSION = 'AUTO'
    COMMENT = 'Pipe-delimited CSV format';

-- JSON format (for semi-structured data)
CREATE OR REPLACE FILE FORMAT RAW.FF_JSON
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = TRUE
    STRIP_NULL_VALUES = TRUE
    IGNORE_UTF8_ERRORS = FALSE
    COMPRESSION = 'AUTO'
    COMMENT = 'Standard JSON format with outer array stripped';

-- JSON raw format (preserves full structure)
CREATE OR REPLACE FILE FORMAT RAW.FF_JSON_RAW
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = FALSE
    STRIP_NULL_VALUES = FALSE
    COMPRESSION = 'AUTO'
    COMMENT = 'Raw JSON format - preserves full structure';

-- Parquet format (preferred for bulk loading - columnar, compressed, schema)
CREATE OR REPLACE FILE FORMAT RAW.FF_PARQUET
    TYPE = 'PARQUET'
    COMPRESSION = 'SNAPPY'
    BINARY_AS_TEXT = FALSE
    COMMENT = 'Parquet format - primary bulk load format';

-- ============================================================================
-- 2. INTERNAL STAGES (for PUT + COPY INTO pattern)
-- ============================================================================

-- Trade data stage (high volume)
CREATE OR REPLACE STAGE RAW.STG_TRADE_DATA
    FILE_FORMAT = RAW.FF_PARQUET
    DIRECTORY = (ENABLE = TRUE)
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
    COMMENT = 'Internal stage for trade transaction data';

-- Sanctions list data stage (critical, lower volume)
CREATE OR REPLACE STAGE RAW.STG_SANCTIONS_LISTS
    FILE_FORMAT = RAW.FF_PARQUET
    DIRECTORY = (ENABLE = TRUE)
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
    COMMENT = 'Internal stage for sanctions list data';

-- Vessel data stage (includes AIS movements)
CREATE OR REPLACE STAGE RAW.STG_VESSEL_DATA
    FILE_FORMAT = RAW.FF_PARQUET
    DIRECTORY = (ENABLE = TRUE)
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
    COMMENT = 'Internal stage for vessel master and movement data';

-- Counterparty data stage
CREATE OR REPLACE STAGE RAW.STG_COUNTERPARTY_DATA
    FILE_FORMAT = RAW.FF_PARQUET
    DIRECTORY = (ENABLE = TRUE)
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
    COMMENT = 'Internal stage for counterparty/entity data';

-- Screening results stage
CREATE OR REPLACE STAGE RAW.STG_SCREENING_DATA
    FILE_FORMAT = RAW.FF_PARQUET
    DIRECTORY = (ENABLE = TRUE)
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
    COMMENT = 'Internal stage for screening results data';

-- ============================================================================
-- 3. EXTERNAL STAGE TEMPLATES (Uncomment and configure as needed)
-- ============================================================================

-- AWS S3 External Stage (requires storage integration)
/*
CREATE OR REPLACE STORAGE INTEGRATION S3_SANCTIONS_INTEGRATION
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::ACCOUNT:role/snowflake-s3-role'
    ENABLED = TRUE
    STORAGE_ALLOWED_LOCATIONS = ('s3://sanctions-data-bucket/');

CREATE OR REPLACE STAGE RAW.STG_S3_EXTERNAL
    URL = 's3://sanctions-data-bucket/incoming/'
    STORAGE_INTEGRATION = S3_SANCTIONS_INTEGRATION
    FILE_FORMAT = RAW.FF_PARQUET
    COMMENT = 'External S3 stage for bulk data ingestion';
*/

-- Azure Blob External Stage (requires storage integration)
/*
CREATE OR REPLACE STORAGE INTEGRATION AZURE_SANCTIONS_INTEGRATION
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = AZURE
    AZURE_TENANT_ID = 'your-tenant-id'
    ENABLED = TRUE
    STORAGE_ALLOWED_LOCATIONS = ('azure://account.blob.core.windows.net/container/');

CREATE OR REPLACE STAGE RAW.STG_AZURE_EXTERNAL
    URL = 'azure://account.blob.core.windows.net/container/sanctions-data/'
    STORAGE_INTEGRATION = AZURE_SANCTIONS_INTEGRATION
    FILE_FORMAT = RAW.FF_PARQUET
    COMMENT = 'External Azure stage for bulk data';
*/

-- GCS External Stage (requires storage integration)
/*
CREATE OR REPLACE STORAGE INTEGRATION GCS_SANCTIONS_INTEGRATION
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = GCS
    ENABLED = TRUE
    STORAGE_ALLOWED_LOCATIONS = ('gcs://sanctions-data-bucket/');

CREATE OR REPLACE STAGE RAW.STG_GCS_EXTERNAL
    URL = 'gcs://sanctions-data-bucket/incoming/'
    STORAGE_INTEGRATION = GCS_SANCTIONS_INTEGRATION
    FILE_FORMAT = RAW.FF_PARQUET
    COMMENT = 'External GCS stage for bulk data';
*/

-- ============================================================================
-- 4. SPECIAL PURPOSE STAGES
-- ============================================================================

-- ML models and artifacts stage
CREATE OR REPLACE STAGE ML.STG_ML_ARTIFACTS
    DIRECTORY = (ENABLE = TRUE)
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
    COMMENT = 'Stage for ML models and artifacts';

-- Streamlit application files stage
CREATE OR REPLACE STAGE CONSUMPTION.STG_STREAMLIT_FILES
    DIRECTORY = (ENABLE = TRUE)
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
    COMMENT = 'Stage for Streamlit application files';

-- ============================================================================
-- 5. VERIFICATION
-- ============================================================================

SHOW STAGES IN SCHEMA RAW;
SHOW FILE FORMATS IN SCHEMA RAW;
