-- ============================================================================
-- 004_snowpipe_setup.sql
-- Snowpipe definitions for automated continuous data ingestion
-- ============================================================================
-- EXECUTION ORDER: Run AFTER 003_raw_tables.sql
-- ============================================================================

USE ROLE SANCTIONS_DATA_ENGINEER;
USE DATABASE SANCTIONS_DEV;
USE SCHEMA RAW;

-- ============================================================================
-- SECTION 1: NOTIFICATION INTEGRATION TEMPLATES
-- ============================================================================
-- Uncomment and configure based on your cloud provider.

-- ── AWS SQS ──
-- CREATE OR REPLACE NOTIFICATION INTEGRATION SANCTIONS_SQS_NOTIFICATION
--     ENABLED = TRUE
--     TYPE = QUEUE
--     NOTIFICATION_PROVIDER = AWS_SQS
--     DIRECTION = OUTBOUND
--     AWS_SQS_ROLE_ARN = 'arn:aws:iam::ACCOUNT_ID:role/sanctions-snowpipe-role'
--     AWS_SQS_ARN = 'arn:aws:sqs:REGION:ACCOUNT_ID:sanctions-snowpipe-queue';

-- ── Azure Storage Queue ──
-- CREATE OR REPLACE NOTIFICATION INTEGRATION SANCTIONS_AZURE_NOTIFICATION
--     ENABLED = TRUE
--     TYPE = QUEUE
--     NOTIFICATION_PROVIDER = AZURE_STORAGE_QUEUE
--     AZURE_STORAGE_QUEUE_PRIMARY_URI = 'https://account.queue.core.windows.net/sanctions-queue'
--     AZURE_TENANT_ID = 'your-tenant-id';

-- ── GCP Pub/Sub ──
-- CREATE OR REPLACE NOTIFICATION INTEGRATION SANCTIONS_GCP_NOTIFICATION
--     ENABLED = TRUE
--     TYPE = QUEUE
--     NOTIFICATION_PROVIDER = GCP_PUBSUB
--     GCP_PUBSUB_SUBSCRIPTION_NAME = 'projects/your-project/subscriptions/sanctions-sub';

-- ============================================================================
-- SECTION 2: SNOWPIPE DEFINITIONS
-- ============================================================================

-- Trades pipe (2.5M records/day)
CREATE OR REPLACE PIPE PIPE_TRADES
    AUTO_INGEST = FALSE  -- Set TRUE when notification integration is configured
    COMMENT = 'Snowpipe for trade transaction ingestion - 2.5M records/day'
AS
    COPY INTO RAW_TRADES
    FROM @STG_TRADE_DATA/
    FILE_FORMAT = (FORMAT_NAME = 'FF_PARQUET')
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    ON_ERROR = 'SKIP_FILE';

-- Vessel movements pipe (10M records/day - HIGH VOLUME)
CREATE OR REPLACE PIPE PIPE_VESSEL_MOVEMENTS
    AUTO_INGEST = FALSE
    COMMENT = 'Snowpipe for AIS vessel movement data - 10M records/day (HIGH VOLUME)'
AS
    COPY INTO RAW_VESSEL_MOVEMENTS
    FROM @STG_VESSEL_DATA/movements/
    FILE_FORMAT = (FORMAT_NAME = 'FF_PARQUET')
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    ON_ERROR = 'SKIP_FILE';

-- Sanctions lists pipe (CRITICAL - regulatory data)
CREATE OR REPLACE PIPE PIPE_SANCTIONS_LISTS
    AUTO_INGEST = FALSE
    COMMENT = 'Snowpipe for sanctions list updates - CRITICAL regulatory data'
AS
    COPY INTO RAW_SANCTIONS_LISTS
    FROM @STG_SANCTIONS_LISTS/
    FILE_FORMAT = (FORMAT_NAME = 'FF_PARQUET')
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    ON_ERROR = 'ABORT_STATEMENT';  -- CRITICAL: Fail on any error for regulatory data

-- Counterparties pipe (KYC data)
CREATE OR REPLACE PIPE PIPE_COUNTERPARTIES
    AUTO_INGEST = FALSE
    COMMENT = 'Snowpipe for counterparty/KYC data ingestion'
AS
    COPY INTO RAW_COUNTERPARTIES
    FROM @STG_COUNTERPARTY_DATA/
    FILE_FORMAT = (FORMAT_NAME = 'FF_PARQUET')
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    ON_ERROR = 'SKIP_FILE';

-- Screening results pipe
CREATE OR REPLACE PIPE PIPE_SCREENING_RESULTS
    AUTO_INGEST = FALSE
    COMMENT = 'Snowpipe for sanctions screening results'
AS
    COPY INTO RAW_SCREENING_RESULTS
    FROM @STG_SCREENING_DATA/
    FILE_FORMAT = (FORMAT_NAME = 'FF_PARQUET')
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    ON_ERROR = 'SKIP_FILE';

-- Vessels pipe (master data)
CREATE OR REPLACE PIPE PIPE_VESSELS
    AUTO_INGEST = FALSE
    COMMENT = 'Snowpipe for vessel master data ingestion'
AS
    COPY INTO RAW_VESSELS
    FROM @STG_VESSEL_DATA/master/
    FILE_FORMAT = (FORMAT_NAME = 'FF_PARQUET')
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    ON_ERROR = 'SKIP_FILE';

-- ============================================================================
-- SECTION 3: PIPE MONITORING
-- ============================================================================

-- Monitoring view for Snowpipe status
CREATE OR REPLACE VIEW SNOWPIPE_MONITORING AS
SELECT
    pipe_catalog_name                   AS database_name,
    pipe_schema_name                    AS schema_name,
    pipe_name,
    definition,
    is_autoingest_enabled,
    notification_channel_name,
    created,
    last_altered
FROM INFORMATION_SCHEMA.PIPES
WHERE pipe_schema_name = 'RAW'
ORDER BY pipe_name;

-- Pipe load history view (last 24 hours)
CREATE OR REPLACE VIEW SNOWPIPE_LOAD_HISTORY AS
SELECT
    pipe_name,
    file_name,
    stage_location,
    row_count,
    row_parsed,
    file_size,
    first_error_message,
    first_error_line_num,
    error_count,
    error_limit,
    status,
    last_load_time
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'RAW_TRADES',
    START_TIME => DATEADD(HOUR, -24, CURRENT_TIMESTAMP())
))
UNION ALL
SELECT
    pipe_name,
    file_name,
    stage_location,
    row_count,
    row_parsed,
    file_size,
    first_error_message,
    first_error_line_num,
    error_count,
    error_limit,
    status,
    last_load_time
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'RAW_VESSEL_MOVEMENTS',
    START_TIME => DATEADD(HOUR, -24, CURRENT_TIMESTAMP())
))
ORDER BY last_load_time DESC;

-- Procedure to check all pipe statuses
CREATE OR REPLACE PROCEDURE CHECK_ALL_PIPES()
RETURNS TABLE (pipe_name VARCHAR, status VARCHAR, pending_file_count NUMBER)
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    res RESULTSET DEFAULT (
        SELECT
            'PIPE_TRADES' AS pipe_name,
            SYSTEM$PIPE_STATUS('PIPE_TRADES') AS status,
            0 AS pending_file_count
        UNION ALL
        SELECT
            'PIPE_VESSEL_MOVEMENTS',
            SYSTEM$PIPE_STATUS('PIPE_VESSEL_MOVEMENTS'),
            0
        UNION ALL
        SELECT
            'PIPE_SANCTIONS_LISTS',
            SYSTEM$PIPE_STATUS('PIPE_SANCTIONS_LISTS'),
            0
        UNION ALL
        SELECT
            'PIPE_COUNTERPARTIES',
            SYSTEM$PIPE_STATUS('PIPE_COUNTERPARTIES'),
            0
        UNION ALL
        SELECT
            'PIPE_SCREENING_RESULTS',
            SYSTEM$PIPE_STATUS('PIPE_SCREENING_RESULTS'),
            0
        UNION ALL
        SELECT
            'PIPE_VESSELS',
            SYSTEM$PIPE_STATUS('PIPE_VESSELS'),
            0
    );
BEGIN
    RETURN TABLE(res);
END;
$$;

-- End of 004_snowpipe_setup.sql
