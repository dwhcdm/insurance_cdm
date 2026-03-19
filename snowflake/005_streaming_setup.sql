-- ============================================================================
-- SCRIPT: 005_streaming_setup.sql
-- PURPOSE: Configure streaming ingestion infrastructure - Streams, Tasks,
--          and Dynamic Tables for real-time CDC processing
-- RUN AS:  SANCTIONS_PLATFORM_ADMIN
-- VERSION: 1.0.0
-- ============================================================================

USE ROLE SANCTIONS_PLATFORM_ADMIN;
USE DATABASE SANCTIONS_DEV;
USE WAREHOUSE SANCTIONS_TRANSFORM_WH_XS;

-- ============================================================================
-- 1. STREAMS (Change Data Capture on Raw Tables)
-- ============================================================================

-- Stream on RAW_TRADES for capturing new/changed trades
CREATE OR REPLACE STREAM RAW.STREAM_TRADES
    ON TABLE RAW.RAW_TRADES
    APPEND_ONLY = TRUE             -- Only captures inserts (optimal for append-heavy tables)
    SHOW_INITIAL_ROWS = FALSE
    COMMENT = 'CDC stream on raw trades - append only for high volume';

-- Stream on RAW_VESSEL_MOVEMENTS (append-only for AIS data)
CREATE OR REPLACE STREAM RAW.STREAM_VESSEL_MOVEMENTS
    ON TABLE RAW.RAW_VESSEL_MOVEMENTS
    APPEND_ONLY = TRUE
    SHOW_INITIAL_ROWS = FALSE
    COMMENT = 'CDC stream on vessel movements - massive volume AIS data';

-- Stream on RAW_SCREENING_RESULTS (standard - captures updates too)
CREATE OR REPLACE STREAM RAW.STREAM_SCREENING_RESULTS
    ON TABLE RAW.RAW_SCREENING_RESULTS
    APPEND_ONLY = FALSE            -- Standard stream: captures INSERT, UPDATE, DELETE
    SHOW_INITIAL_ROWS = FALSE
    COMMENT = 'CDC stream on screening results - includes updates';

-- Stream on RAW_SANCTIONS_LISTS (standard - critical for list updates)
CREATE OR REPLACE STREAM RAW.STREAM_SANCTIONS_LISTS
    ON TABLE RAW.RAW_SANCTIONS_LISTS
    APPEND_ONLY = FALSE
    SHOW_INITIAL_ROWS = FALSE
    COMMENT = 'CDC stream on sanctions lists - critical updates';

-- Stream on RAW_COUNTERPARTIES (standard - tracks KYC changes)
CREATE OR REPLACE STREAM RAW.STREAM_COUNTERPARTIES
    ON TABLE RAW.RAW_COUNTERPARTIES
    APPEND_ONLY = FALSE
    SHOW_INITIAL_ROWS = FALSE
    COMMENT = 'CDC stream on counterparty data - KYC updates';

-- Stream on streaming landing tables
CREATE OR REPLACE STREAM RAW.STREAM_VESSEL_POSITIONS_RT
    ON TABLE RAW.STREAMING_VESSEL_POSITIONS
    APPEND_ONLY = TRUE
    COMMENT = 'CDC stream on real-time vessel positions';

CREATE OR REPLACE STREAM RAW.STREAM_TRADE_EVENTS_RT
    ON TABLE RAW.STREAMING_TRADE_EVENTS
    APPEND_ONLY = TRUE
    COMMENT = 'CDC stream on real-time trade events';

CREATE OR REPLACE STREAM RAW.STREAM_SANCTIONS_ALERTS_RT
    ON TABLE RAW.STREAMING_SANCTIONS_ALERTS
    APPEND_ONLY = TRUE
    COMMENT = 'CDC stream on real-time sanctions alerts';

-- ============================================================================
-- 2. TASKS (Scheduled Processing of Stream Data)
-- ============================================================================

-- Task: Process new trades into staging (runs every 5 minutes)
CREATE OR REPLACE TASK RAW.TASK_PROCESS_NEW_TRADES
    WAREHOUSE = SANCTIONS_TRANSFORM_WH_XS
    SCHEDULE = '5 MINUTE'
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    WHEN SYSTEM$STREAM_HAS_DATA('RAW.STREAM_TRADES')
    COMMENT = 'Process new trades from stream into staging layer'
AS
    INSERT INTO STAGING.STG_TRADES_INCREMENTAL
    SELECT
        trade_id,
        trade_reference,
        trade_timestamp,
        trade_date,
        settlement_date,
        UPPER(trade_type) AS trade_type,
        UPPER(trade_status) AS trade_status,
        buyer_counterparty_id,
        seller_counterparty_id,
        UPPER(commodity_code) AS commodity_code,
        UPPER(commodity_name) AS commodity_name,
        UPPER(commodity_group) AS commodity_group,
        quantity_mt,
        price_per_mt_usd,
        total_value_usd,
        UPPER(currency) AS currency,
        fx_rate,
        UPPER(incoterm) AS incoterm,
        UPPER(origin_country) AS origin_country,
        UPPER(destination_country) AS destination_country,
        UPPER(loading_port) AS loading_port,
        UPPER(discharge_port) AS discharge_port,
        vessel_id,
        vessel_flagged,
        sanctions_risk_score,
        UPPER(screening_status) AS screening_status,
        UPPER(booking_entity) AS booking_entity,
        trader_id,
        UPPER(desk) AS desk,
        UPPER(source_system) AS source_system,
        created_at,
        updated_at,
        _loaded_at,
        _file_name,
        _file_row_number,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS _processed_at
    FROM RAW.STREAM_TRADES
    WHERE trade_id IS NOT NULL
      AND trade_date IS NOT NULL
      AND total_value_usd > 0;


-- Task: Process real-time vessel positions (runs every minute)
CREATE OR REPLACE TASK RAW.TASK_PROCESS_VESSEL_POSITIONS
    WAREHOUSE = SANCTIONS_TRANSFORM_WH_XS
    SCHEDULE = '1 MINUTE'
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    WHEN SYSTEM$STREAM_HAS_DATA('RAW.STREAM_VESSEL_POSITIONS_RT')
    COMMENT = 'Process real-time vessel positions for dark activity detection'
AS
    MERGE INTO STAGING.VESSEL_POSITION_LATEST AS target
    USING (
        SELECT
            vessel_id,
            timestamp AS position_timestamp,
            latitude,
            longitude,
            speed_knots,
            heading,
            source,
            received_at
        FROM RAW.STREAM_VESSEL_POSITIONS_RT
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY vessel_id
            ORDER BY timestamp DESC
        ) = 1
    ) AS source
    ON target.vessel_id = source.vessel_id
    WHEN MATCHED THEN UPDATE SET
        target.position_timestamp = source.position_timestamp,
        target.latitude = source.latitude,
        target.longitude = source.longitude,
        target.speed_knots = source.speed_knots,
        target.heading = source.heading,
        target.source = source.source,
        target.received_at = source.received_at,
        target._updated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        vessel_id, position_timestamp, latitude, longitude,
        speed_knots, heading, source, received_at, _updated_at
    ) VALUES (
        source.vessel_id, source.position_timestamp, source.latitude, source.longitude,
        source.speed_knots, source.heading, source.source, source.received_at, CURRENT_TIMESTAMP()
    );


-- Task: Process sanctions alerts (runs every minute - critical)
CREATE OR REPLACE TASK RAW.TASK_PROCESS_SANCTIONS_ALERTS
    WAREHOUSE = SANCTIONS_TRANSFORM_WH_XS
    SCHEDULE = '1 MINUTE'
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    WHEN SYSTEM$STREAM_HAS_DATA('RAW.STREAM_SANCTIONS_ALERTS_RT')
    COMMENT = 'Process real-time sanctions alerts - critical priority'
AS
    INSERT INTO STAGING.SANCTIONS_ALERTS_PROCESSED
    SELECT
        alert_id,
        alert_type,
        severity,
        entity_id,
        entity_name,
        sanctions_list,
        alert_timestamp,
        alert_data,
        received_at,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS _processed_at,
        CASE
            WHEN severity IN ('CRITICAL', 'HIGH') THEN TRUE
            ELSE FALSE
        END AS requires_immediate_action
    FROM RAW.STREAM_SANCTIONS_ALERTS_RT;


-- ============================================================================
-- 3. STAGING TABLES FOR STREAM PROCESSING
-- ============================================================================

-- Create staging tables that tasks write to
CREATE OR REPLACE TABLE STAGING.STG_TRADES_INCREMENTAL LIKE RAW.RAW_TRADES;
ALTER TABLE STAGING.STG_TRADES_INCREMENTAL ADD COLUMN _processed_at TIMESTAMP_NTZ;

CREATE OR REPLACE TABLE STAGING.VESSEL_POSITION_LATEST (
    vessel_id               VARCHAR(20)     NOT NULL,
    position_timestamp      TIMESTAMP_NTZ   NOT NULL,
    latitude                NUMBER(10,6),
    longitude               NUMBER(10,6),
    speed_knots             NUMBER(5,1),
    heading                 NUMBER(5,1),
    source                  VARCHAR(20),
    received_at             TIMESTAMP_NTZ,
    _updated_at             TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Latest vessel position - continuously updated by streaming task';

CREATE OR REPLACE TABLE STAGING.SANCTIONS_ALERTS_PROCESSED (
    alert_id                VARCHAR(50)     NOT NULL,
    alert_type              VARCHAR(30),
    severity                VARCHAR(10),
    entity_id               VARCHAR(50),
    entity_name             VARCHAR(500),
    sanctions_list          VARCHAR(50),
    alert_timestamp         TIMESTAMP_NTZ   NOT NULL,
    alert_data              VARIANT,
    received_at             TIMESTAMP_NTZ,
    _processed_at           TIMESTAMP_NTZ,
    requires_immediate_action BOOLEAN       DEFAULT FALSE
)
COMMENT = 'Processed sanctions alerts from real-time stream';


-- ============================================================================
-- 4. DYNAMIC TABLES (Snowflake-native streaming transformations)
-- ============================================================================

-- Dynamic table for near-real-time trade risk scoring
CREATE OR REPLACE DYNAMIC TABLE STAGING.DT_TRADE_RISK_REALTIME
    TARGET_LAG = '5 minutes'
    WAREHOUSE = SANCTIONS_TRANSFORM_WH_XS
    COMMENT = 'Near-real-time trade risk assessment via Dynamic Table'
AS
    SELECT
        t.trade_id,
        t.trade_date,
        t.buyer_counterparty_id,
        t.seller_counterparty_id,
        t.commodity_code,
        t.commodity_group,
        t.total_value_usd,
        t.origin_country,
        t.destination_country,
        t.vessel_id,
        t.sanctions_risk_score,
        t.screening_status,

        -- Real-time risk classification
        CASE
            WHEN t.origin_country IN ('IR', 'KP', 'SY', 'CU') OR
                 t.destination_country IN ('IR', 'KP', 'SY', 'CU') THEN 'PROHIBITED'
            WHEN t.sanctions_risk_score >= 80 THEN 'CRITICAL'
            WHEN t.sanctions_risk_score >= 60 THEN 'HIGH'
            WHEN t.sanctions_risk_score >= 40 THEN 'ELEVATED'
            ELSE 'STANDARD'
        END AS realtime_risk_tier,

        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS _evaluated_at

    FROM RAW.RAW_TRADES t
    WHERE t._loaded_at >= DATEADD(day, -1, CURRENT_TIMESTAMP());


-- Dynamic table for vessel dark activity detection
CREATE OR REPLACE DYNAMIC TABLE STAGING.DT_VESSEL_DARK_ACTIVITY
    TARGET_LAG = '2 minutes'
    WAREHOUSE = SANCTIONS_TRANSFORM_WH_XS
    COMMENT = 'Near-real-time vessel dark activity detection'
AS
    SELECT
        vessel_id,
        COUNT(*) AS total_positions,
        SUM(CASE WHEN is_dark_activity THEN 1 ELSE 0 END) AS dark_positions,
        SUM(COALESCE(dark_duration_hours, 0)) AS total_dark_hours,
        MAX(zone_risk_score) AS max_zone_risk,
        MAX(timestamp) AS last_seen,
        DATEDIFF('minute', MAX(timestamp), CURRENT_TIMESTAMP()) AS minutes_since_last_seen,
        CASE
            WHEN DATEDIFF('minute', MAX(timestamp), CURRENT_TIMESTAMP()) > 120 THEN 'POTENTIALLY_DARK'
            WHEN SUM(CASE WHEN is_dark_activity THEN 1 ELSE 0 END) > 5 THEN 'DARK_HISTORY'
            ELSE 'NORMAL'
        END AS dark_status
    FROM RAW.RAW_VESSEL_MOVEMENTS
    WHERE timestamp >= DATEADD(day, -7, CURRENT_TIMESTAMP())
    GROUP BY vessel_id;


-- ============================================================================
-- 5. TASK MANAGEMENT
-- ============================================================================

-- Resume all tasks (they are created in suspended state)
ALTER TASK RAW.TASK_PROCESS_NEW_TRADES RESUME;
ALTER TASK RAW.TASK_PROCESS_VESSEL_POSITIONS RESUME;
ALTER TASK RAW.TASK_PROCESS_SANCTIONS_ALERTS RESUME;


-- ============================================================================
-- 6. MONITORING VIEWS
-- ============================================================================

-- Stream lag monitoring
CREATE OR REPLACE VIEW GOVERNANCE.STREAM_MONITORING AS
SELECT
    'STREAM_TRADES' AS stream_name,
    SYSTEM$STREAM_HAS_DATA('RAW.STREAM_TRADES') AS has_data,
    'RAW.RAW_TRADES' AS source_table
UNION ALL
SELECT
    'STREAM_VESSEL_MOVEMENTS',
    SYSTEM$STREAM_HAS_DATA('RAW.STREAM_VESSEL_MOVEMENTS'),
    'RAW.RAW_VESSEL_MOVEMENTS'
UNION ALL
SELECT
    'STREAM_SCREENING_RESULTS',
    SYSTEM$STREAM_HAS_DATA('RAW.STREAM_SCREENING_RESULTS'),
    'RAW.RAW_SCREENING_RESULTS'
UNION ALL
SELECT
    'STREAM_SANCTIONS_LISTS',
    SYSTEM$STREAM_HAS_DATA('RAW.STREAM_SANCTIONS_LISTS'),
    'RAW.RAW_SANCTIONS_LISTS'
UNION ALL
SELECT
    'STREAM_COUNTERPARTIES',
    SYSTEM$STREAM_HAS_DATA('RAW.STREAM_COUNTERPARTIES'),
    'RAW.RAW_COUNTERPARTIES';

-- Task execution history
CREATE OR REPLACE VIEW GOVERNANCE.TASK_EXECUTION_HISTORY AS
SELECT
    NAME AS task_name,
    DATABASE_NAME,
    SCHEMA_NAME,
    STATE,
    QUERY_START_TIME,
    COMPLETED_TIME,
    DATEDIFF('second', QUERY_START_TIME, COMPLETED_TIME) AS duration_seconds,
    ERROR_CODE,
    ERROR_MESSAGE,
    RETURN_VALUE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD(hours, -24, CURRENT_TIMESTAMP()),
    RESULT_LIMIT => 100
))
ORDER BY QUERY_START_TIME DESC;


-- ============================================================================
-- 7. VERIFICATION
-- ============================================================================

SHOW STREAMS IN SCHEMA RAW;
SHOW TASKS IN SCHEMA RAW;
SHOW DYNAMIC TABLES IN SCHEMA STAGING;

SELECT * FROM GOVERNANCE.STREAM_MONITORING;
SELECT * FROM GOVERNANCE.TASK_EXECUTION_HISTORY LIMIT 10;
