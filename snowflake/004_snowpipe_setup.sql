-- ============================================================================
-- SCRIPT: 004_snowpipe_setup.sql
-- PURPOSE: Configure Snowpipe for continuous/streaming data ingestion
-- RUN AS:  SANCTIONS_PLATFORM_ADMIN
-- VERSION: 1.0.0
-- ============================================================================

USE ROLE SANCTIONS_PLATFORM_ADMIN;
USE DATABASE SANCTIONS_DEV;
USE WAREHOUSE SANCTIONS_ADMIN_WH;

-- ============================================================================
-- 1. NOTIFICATION INTEGRATION (for cloud event triggers)
-- ============================================================================

-- For AWS S3 (if using external stage)
/*
CREATE OR REPLACE NOTIFICATION INTEGRATION SANCTIONS_S3_NOTIFICATION
    ENABLED = TRUE
    TYPE = QUEUE
    NOTIFICATION_PROVIDER = AWS_SQS
    DIRECTION = INBOUND
    AWS_SQS_ARN = 'arn:aws:sqs:us-east-1:ACCOUNT:sanctions-snowpipe-queue'
    AWS_SQS_ROLE_ARN = 'arn:aws:iam::ACCOUNT:role/snowflake-sqs-role';
*/

-- For Azure (if using external stage)
/*
CREATE OR REPLACE NOTIFICATION INTEGRATION SANCTIONS_AZURE_NOTIFICATION
    ENABLED = TRUE
    TYPE = QUEUE
    NOTIFICATION_PROVIDER = AZURE_STORAGE_QUEUE
    AZURE_STORAGE_QUEUE_PRIMARY_URI = 'https://account.queue.core.windows.net/sanctions-queue'
    AZURE_TENANT_ID = 'tenant-id';
*/

-- For GCP (if using external stage)
/*
CREATE OR REPLACE NOTIFICATION INTEGRATION SANCTIONS_GCS_NOTIFICATION
    ENABLED = TRUE
    TYPE = QUEUE
    NOTIFICATION_PROVIDER = GCP_PUBSUB
    GCP_PUBSUB_SUBSCRIPTION_NAME = 'projects/project-id/subscriptions/sanctions-sub';
*/

-- ============================================================================
-- 2. SNOWPIPE FOR TRADE DATA (High Volume - Real-time)
-- ============================================================================

CREATE OR REPLACE PIPE RAW.PIPE_TRADES
    AUTO_INGEST = TRUE
    -- AWS_SNS_TOPIC = 'arn:aws:sns:...'  -- Uncomment for cloud auto-ingest
    COMMENT = 'Snowpipe for real-time trade ingestion'
AS
COPY INTO RAW.RAW_TRADES
FROM @RAW.STG_TRADE_DATA/
FILE_FORMAT = (TYPE = 'PARQUET')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = 'SKIP_FILE';

-- Check pipe status
SELECT SYSTEM$PIPE_STATUS('RAW.PIPE_TRADES');


-- ============================================================================
-- 3. SNOWPIPE FOR VESSEL MOVEMENTS (Very High Volume - AIS Data)
-- ============================================================================

CREATE OR REPLACE PIPE RAW.PIPE_VESSEL_MOVEMENTS
    AUTO_INGEST = TRUE
    COMMENT = 'Snowpipe for AIS vessel movement data - high volume'
AS
COPY INTO RAW.RAW_VESSEL_MOVEMENTS
FROM @RAW.STG_VESSEL_DATA/movements/
FILE_FORMAT = (TYPE = 'PARQUET')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = 'SKIP_FILE';


-- ============================================================================
-- 4. SNOWPIPE FOR SANCTIONS LIST UPDATES (Low Volume - Critical)
-- ============================================================================

CREATE OR REPLACE PIPE RAW.PIPE_SANCTIONS_LISTS
    AUTO_INGEST = TRUE
    COMMENT = 'Snowpipe for sanctions list updates'
AS
COPY INTO RAW.RAW_SANCTIONS_LISTS
FROM @RAW.STG_SANCTIONS_LISTS/
FILE_FORMAT = (TYPE = 'PARQUET')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = 'ABORT_STATEMENT';  -- Strict for sanctions data


-- ============================================================================
-- 5. SNOWPIPE FOR COUNTERPARTY DATA
-- ============================================================================

CREATE OR REPLACE PIPE RAW.PIPE_COUNTERPARTIES
    AUTO_INGEST = TRUE
    COMMENT = 'Snowpipe for counterparty/KYC data'
AS
COPY INTO RAW.RAW_COUNTERPARTIES
FROM @RAW.STG_COUNTERPARTY_DATA/
FILE_FORMAT = (TYPE = 'PARQUET')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = 'SKIP_FILE';


-- ============================================================================
-- 6. SNOWPIPE FOR SCREENING RESULTS
-- ============================================================================

CREATE OR REPLACE PIPE RAW.PIPE_SCREENING_RESULTS
    AUTO_INGEST = TRUE
    COMMENT = 'Snowpipe for screening results'
AS
COPY INTO RAW.RAW_SCREENING_RESULTS
FROM @RAW.STG_SCREENING_DATA/
FILE_FORMAT = (TYPE = 'PARQUET')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = 'SKIP_FILE';


-- ============================================================================
-- 7. SNOWPIPE FOR VESSEL MASTER DATA
-- ============================================================================

CREATE OR REPLACE PIPE RAW.PIPE_VESSELS
    AUTO_INGEST = TRUE
    COMMENT = 'Snowpipe for vessel master data updates'
AS
COPY INTO RAW.RAW_VESSELS
FROM @RAW.STG_VESSEL_DATA/master/
FILE_FORMAT = (TYPE = 'PARQUET')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = 'SKIP_FILE';


-- ============================================================================
-- 8. MONITORING VIEWS FOR SNOWPIPE
-- ============================================================================

CREATE OR REPLACE VIEW GOVERNANCE.SNOWPIPE_MONITORING AS
SELECT
    PIPE_CATALOG_NAME AS database_name,
    PIPE_SCHEMA_NAME AS schema_name,
    PIPE_NAME AS pipe_name,
    PIPE_OWNER AS owner,
    IS_AUTOINGEST_ENABLED AS auto_ingest,
    NOTIFICATION_CHANNEL_NAME AS notification_channel,
    DEFINITION AS pipe_definition,
    CREATED AS created_at,
    LAST_ALTERED AS last_altered_at,
    COMMENT
FROM INFORMATION_SCHEMA.PIPES
WHERE PIPE_SCHEMA_NAME = 'RAW'
ORDER BY PIPE_NAME;

-- Snowpipe load history monitoring (last 24 hours)
CREATE OR REPLACE VIEW GOVERNANCE.SNOWPIPE_LOAD_HISTORY AS
SELECT
    pipe_name,
    pipe_received_time,
    first_error_message,
    first_error_line_num,
    first_error_character_pos,
    first_error_column_name,
    error_count,
    error_limit,
    status,
    table_catalog_name,
    table_schema_name,
    table_name,
    last_inserts_count AS rows_inserted,
    last_insert_size AS bytes_inserted,
    DATEDIFF('minute', pipe_received_time, CURRENT_TIMESTAMP()) AS minutes_since_received
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'RAW.RAW_TRADES',
    START_TIME => DATEADD(hours, -24, CURRENT_TIMESTAMP())
));

-- Pipe status check procedure
CREATE OR REPLACE PROCEDURE GOVERNANCE.CHECK_ALL_PIPES()
RETURNS TABLE(pipe_name VARCHAR, status VARCHAR, pending_file_count NUMBER)
LANGUAGE SQL
AS
$$
DECLARE
    result RESULTSET;
BEGIN
    result := (
        SELECT
            'PIPE_TRADES' AS pipe_name,
            PARSE_JSON(SYSTEM$PIPE_STATUS('RAW.PIPE_TRADES')):executionState::VARCHAR AS status,
            PARSE_JSON(SYSTEM$PIPE_STATUS('RAW.PIPE_TRADES')):pendingFileCount::NUMBER AS pending_file_count
        UNION ALL
        SELECT
            'PIPE_VESSEL_MOVEMENTS',
            PARSE_JSON(SYSTEM$PIPE_STATUS('RAW.PIPE_VESSEL_MOVEMENTS')):executionState::VARCHAR,
            PARSE_JSON(SYSTEM$PIPE_STATUS('RAW.PIPE_VESSEL_MOVEMENTS')):pendingFileCount::NUMBER
        UNION ALL
        SELECT
            'PIPE_SANCTIONS_LISTS',
            PARSE_JSON(SYSTEM$PIPE_STATUS('RAW.PIPE_SANCTIONS_LISTS')):executionState::VARCHAR,
            PARSE_JSON(SYSTEM$PIPE_STATUS('RAW.PIPE_SANCTIONS_LISTS')):pendingFileCount::NUMBER
        UNION ALL
        SELECT
            'PIPE_COUNTERPARTIES',
            PARSE_JSON(SYSTEM$PIPE_STATUS('RAW.PIPE_COUNTERPARTIES')):executionState::VARCHAR,
            PARSE_JSON(SYSTEM$PIPE_STATUS('RAW.PIPE_COUNTERPARTIES')):pendingFileCount::NUMBER
        UNION ALL
        SELECT
            'PIPE_SCREENING_RESULTS',
            PARSE_JSON(SYSTEM$PIPE_STATUS('RAW.PIPE_SCREENING_RESULTS')):executionState::VARCHAR,
            PARSE_JSON(SYSTEM$PIPE_STATUS('RAW.PIPE_SCREENING_RESULTS')):pendingFileCount::NUMBER
        UNION ALL
        SELECT
            'PIPE_VESSELS',
            PARSE_JSON(SYSTEM$PIPE_STATUS('RAW.PIPE_VESSELS')):executionState::VARCHAR,
            PARSE_JSON(SYSTEM$PIPE_STATUS('RAW.PIPE_VESSELS')):pendingFileCount::NUMBER
    );
    RETURN TABLE(result);
END;
$$;


-- ============================================================================
-- 9. VERIFICATION
-- ============================================================================

SHOW PIPES IN SCHEMA RAW;
CALL GOVERNANCE.CHECK_ALL_PIPES();
