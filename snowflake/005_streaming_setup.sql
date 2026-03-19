-- ============================================================================
-- 005_streaming_setup.sql
-- Streams, Tasks, and Dynamic Tables for CDC and near-real-time processing
-- ============================================================================
-- EXECUTION ORDER: Run AFTER 004_snowpipe_setup.sql
-- ============================================================================

USE ROLE SANCTIONS_DATA_ENGINEER;
USE DATABASE SANCTIONS_DEV;
USE SCHEMA RAW;

-- ============================================================================
-- SECTION 1: STREAMS (Change Data Capture)
-- ============================================================================

-- Stream on RAW_TRADES (append-only for new trades)
CREATE OR REPLACE STREAM STREAM_TRADES
    ON TABLE RAW_TRADES
    APPEND_ONLY = TRUE
    SHOW_INITIAL_ROWS = FALSE
    COMMENT = 'Captures new trade inserts for incremental processing';

-- Stream on RAW_VESSEL_MOVEMENTS (append-only for AIS data)
CREATE OR REPLACE STREAM STREAM_VESSEL_MOVEMENTS
    ON TABLE RAW_VESSEL_MOVEMENTS
    APPEND_ONLY = TRUE
    SHOW_INITIAL_ROWS = FALSE
    COMMENT = 'Captures new vessel movement records for incremental processing';

-- Stream on RAW_SCREENING_RESULTS (standard - captures inserts and updates)
CREATE OR REPLACE STREAM STREAM_SCREENING_RESULTS
    ON TABLE RAW_SCREENING_RESULTS
    APPEND_ONLY = FALSE
    SHOW_INITIAL_ROWS = FALSE
    COMMENT = 'Captures screening result changes including disposition updates';

-- Stream on RAW_SANCTIONS_LISTS (standard - captures all changes)
CREATE OR REPLACE STREAM STREAM_SANCTIONS_LISTS
    ON TABLE RAW_SANCTIONS_LISTS
    APPEND_ONLY = FALSE
    SHOW_INITIAL_ROWS = FALSE
    COMMENT = 'Captures sanctions list additions, removals, and modifications';

-- Stream on RAW_COUNTERPARTIES (standard - captures KYC updates)
CREATE OR REPLACE STREAM STREAM_COUNTERPARTIES
    ON TABLE RAW_COUNTERPARTIES
    APPEND_ONLY = FALSE
    SHOW_INITIAL_ROWS = FALSE
    COMMENT = 'Captures counterparty data changes for KYC refresh processing';

-- Streams on streaming landing tables
CREATE OR REPLACE STREAM STREAM_VESSEL_POSITIONS_RT
    ON TABLE STREAMING_VESSEL_POSITIONS
    APPEND_ONLY = TRUE
    COMMENT = 'Real-time stream on AIS vessel position updates';

CREATE OR REPLACE STREAM STREAM_TRADE_EVENTS_RT
    ON TABLE STREAMING_TRADE_EVENTS
    APPEND_ONLY = TRUE
    COMMENT = 'Real-time stream on trade lifecycle events';

CREATE OR REPLACE STREAM STREAM_SANCTIONS_ALERTS_RT
    ON TABLE STREAMING_SANCTIONS_ALERTS
    APPEND_ONLY = TRUE
    COMMENT = 'Real-time stream on sanctions screening alerts';

-- ============================================================================
-- SECTION 2: STAGING TABLES FOR STREAM PROCESSING
-- ============================================================================

USE SCHEMA STAGING;

-- Incremental trade staging table
CREATE OR REPLACE TRANSIENT TABLE STG_TRADES_INCREMENTAL (
    trade_id                VARCHAR(20)     NOT NULL,
    trade_reference         VARCHAR(50),
    trade_timestamp         TIMESTAMP_NTZ   NOT NULL,
    trade_date              DATE            NOT NULL,
    trade_type              VARCHAR(20),
    trade_status            VARCHAR(20),
    buyer_counterparty_id   VARCHAR(20),
    seller_counterparty_id  VARCHAR(20),
    commodity_code          VARCHAR(20),
    commodity_group         VARCHAR(50),
    total_value_usd         NUMBER(18,2),
    sanctions_risk_score    NUMBER(5,2),
    screening_status        VARCHAR(20),
    vessel_id               VARCHAR(20),
    vessel_flagged          BOOLEAN,
    _processed_at           TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
)
DATA_RETENTION_TIME_IN_DAYS = 1
COMMENT = 'Staging table for incrementally processed trades from stream';

-- Latest vessel position (maintained via stream processing)
CREATE OR REPLACE TABLE VESSEL_POSITION_LATEST (
    vessel_id               VARCHAR(20)     NOT NULL,
    latest_timestamp        TIMESTAMP_NTZ   NOT NULL,
    latitude                NUMBER(10,6)    NOT NULL,
    longitude               NUMBER(11,6)    NOT NULL,
    speed_knots             NUMBER(6,1),
    heading                 NUMBER(5,1),
    _updated_at             TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (vessel_id)
)
COMMENT = 'Latest known position for each vessel - maintained by stream task';

-- Processed sanctions alerts
CREATE OR REPLACE TABLE SANCTIONS_ALERTS_PROCESSED (
    alert_id                VARCHAR(20)     NOT NULL,
    alert_type              VARCHAR(30)     NOT NULL,
    severity                VARCHAR(10)     NOT NULL,
    alert_timestamp         TIMESTAMP_NTZ   NOT NULL,
    alert_payload           VARIANT         NOT NULL,
    is_acknowledged         BOOLEAN         DEFAULT FALSE,
    acknowledged_by         VARCHAR(50),
    acknowledged_at         TIMESTAMP_NTZ,
    _processed_at           TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (alert_id)
)
COMMENT = 'Processed sanctions alerts with acknowledgement tracking';

-- ============================================================================
-- SECTION 3: TASKS (Scheduled Stream Processing)
-- ============================================================================

USE SCHEMA RAW;

-- Task: Process new trades every 5 minutes
CREATE OR REPLACE TASK TASK_PROCESS_NEW_TRADES
    WAREHOUSE = SANCTIONS_TRANSFORM_WH_XS
    SCHEDULE = '5 MINUTE'
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    WHEN SYSTEM$STREAM_HAS_DATA('STREAM_TRADES')
    COMMENT = 'Process new trades from stream into staging every 5 minutes'
AS
    INSERT INTO STAGING.STG_TRADES_INCREMENTAL (
        trade_id, trade_reference, trade_timestamp, trade_date,
        trade_type, trade_status, buyer_counterparty_id,
        seller_counterparty_id, commodity_code, commodity_group,
        total_value_usd, sanctions_risk_score, screening_status,
        vessel_id, vessel_flagged
    )
    SELECT
        trade_id, trade_reference, trade_timestamp, trade_date,
        trade_type, trade_status, buyer_counterparty_id,
        seller_counterparty_id, commodity_code, commodity_group,
        total_value_usd, sanctions_risk_score, screening_status,
        vessel_id, vessel_flagged
    FROM STREAM_TRADES;

-- Task: Process vessel positions every 1 minute (MERGE for latest position)
CREATE OR REPLACE TASK TASK_PROCESS_VESSEL_POSITIONS
    WAREHOUSE = SANCTIONS_TRANSFORM_WH_XS
    SCHEDULE = '1 MINUTE'
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    WHEN SYSTEM$STREAM_HAS_DATA('STREAM_VESSEL_POSITIONS_RT')
    COMMENT = 'Merge latest vessel positions from streaming every 1 minute'
AS
    MERGE INTO STAGING.VESSEL_POSITION_LATEST tgt
    USING (
        SELECT
            vessel_id,
            timestamp          AS latest_timestamp,
            latitude,
            longitude,
            speed_knots,
            heading
        FROM STREAM_VESSEL_POSITIONS_RT
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY vessel_id ORDER BY timestamp DESC
        ) = 1
    ) src
    ON tgt.vessel_id = src.vessel_id
    WHEN MATCHED AND src.latest_timestamp > tgt.latest_timestamp THEN
        UPDATE SET
            tgt.latest_timestamp = src.latest_timestamp,
            tgt.latitude         = src.latitude,
            tgt.longitude        = src.longitude,
            tgt.speed_knots      = src.speed_knots,
            tgt.heading          = src.heading,
            tgt._updated_at      = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (vessel_id, latest_timestamp, latitude, longitude, speed_knots, heading)
        VALUES (src.vessel_id, src.latest_timestamp, src.latitude, src.longitude, src.speed_knots, src.heading);

-- Task: Process sanctions alerts every 1 minute (CRITICAL path)
CREATE OR REPLACE TASK TASK_PROCESS_SANCTIONS_ALERTS
    WAREHOUSE = SANCTIONS_TRANSFORM_WH_XS
    SCHEDULE = '1 MINUTE'
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    WHEN SYSTEM$STREAM_HAS_DATA('STREAM_SANCTIONS_ALERTS_RT')
    COMMENT = 'Process sanctions alerts from streaming - CRITICAL 1-minute SLA'
AS
    INSERT INTO STAGING.SANCTIONS_ALERTS_PROCESSED (
        alert_id, alert_type, severity, alert_timestamp, alert_payload
    )
    SELECT
        alert_id, alert_type, severity, alert_timestamp, alert_payload
    FROM STREAM_SANCTIONS_ALERTS_RT;

-- NOTE: Tasks are created in SUSPENDED state by default.
-- Execute the following to activate them:
-- ALTER TASK TASK_PROCESS_NEW_TRADES RESUME;
-- ALTER TASK TASK_PROCESS_VESSEL_POSITIONS RESUME;
-- ALTER TASK TASK_PROCESS_SANCTIONS_ALERTS RESUME;

-- ============================================================================
-- SECTION 4: DYNAMIC TABLES (Near-Real-Time Materialized Views)
-- ============================================================================

USE SCHEMA CURATED;

-- Dynamic Table: Near-real-time trade risk classification (5-min lag)
CREATE OR REPLACE DYNAMIC TABLE DT_TRADE_RISK_REALTIME
    TARGET_LAG = '5 minutes'
    WAREHOUSE = SANCTIONS_TRANSFORM_WH_XS
    COMMENT = 'Near-real-time trade risk classification with 5-minute lag'
AS
    SELECT
        t.trade_id,
        t.trade_date,
        t.trade_type,
        t.buyer_counterparty_id,
        t.seller_counterparty_id,
        t.commodity_code,
        t.commodity_group,
        t.total_value_usd,
        t.sanctions_risk_score,
        t.screening_status,
        t.vessel_id,
        t.vessel_flagged,

        -- Dynamic risk classification
        CASE
            WHEN t.sanctions_risk_score >= 80 THEN 'CRITICAL'
            WHEN t.sanctions_risk_score >= 60 THEN 'HIGH'
            WHEN t.sanctions_risk_score >= 40 THEN 'ELEVATED'
            WHEN t.sanctions_risk_score >= 20 THEN 'MEDIUM'
            ELSE 'LOW'
        END AS risk_tier,

        -- Counterparty risk flags
        COALESCE(bc.is_sanctioned, FALSE) AS buyer_sanctioned,
        COALESCE(sc.is_sanctioned, FALSE) AS seller_sanctioned,
        COALESCE(bc.is_pep, FALSE) AS buyer_is_pep,
        COALESCE(sc.is_pep, FALSE) AS seller_is_pep,

        t._loaded_at

    FROM RAW.RAW_TRADES t
    LEFT JOIN RAW.RAW_COUNTERPARTIES bc ON t.buyer_counterparty_id = bc.counterparty_id
    LEFT JOIN RAW.RAW_COUNTERPARTIES sc ON t.seller_counterparty_id = sc.counterparty_id;

-- Dynamic Table: Vessel dark activity detection (2-min lag)
CREATE OR REPLACE DYNAMIC TABLE DT_VESSEL_DARK_ACTIVITY
    TARGET_LAG = '2 minutes'
    WAREHOUSE = SANCTIONS_TRANSFORM_WH_XS
    COMMENT = 'Near-real-time vessel dark activity detection with 2-minute lag'
AS
    SELECT
        vm.vessel_id,
        v.vessel_name,
        v.imo_number,
        v.flag_state,
        v.vessel_type,
        v.is_flagged,
        COUNT(*) AS total_movements_24h,
        COUNT_IF(vm.is_dark_activity) AS dark_events_24h,
        SUM(COALESCE(vm.dark_duration_hours, 0)) AS total_dark_hours_24h,
        COUNT_IF(vm.is_near_sanctioned_zone) AS sanctioned_zone_events_24h,
        AVG(vm.zone_risk_score) AS avg_zone_risk_24h,
        MAX(vm.zone_risk_score) AS max_zone_risk_24h,
        MAX(vm.timestamp) AS last_seen_at,

        -- Composite alert level
        CASE
            WHEN v.is_flagged AND COUNT_IF(vm.is_dark_activity) > 0 THEN 'CRITICAL'
            WHEN COUNT_IF(vm.is_dark_activity) > 3 THEN 'HIGH'
            WHEN COUNT_IF(vm.is_near_sanctioned_zone) > 5 THEN 'HIGH'
            WHEN COUNT_IF(vm.is_dark_activity) > 0 THEN 'ELEVATED'
            WHEN COUNT_IF(vm.is_near_sanctioned_zone) > 0 THEN 'MEDIUM'
            ELSE 'NORMAL'
        END AS alert_level

    FROM RAW.RAW_VESSEL_MOVEMENTS vm
    JOIN RAW.RAW_VESSELS v ON vm.vessel_id = v.vessel_id
    WHERE vm.timestamp >= DATEADD(HOUR, -24, CURRENT_TIMESTAMP())
    GROUP BY
        vm.vessel_id, v.vessel_name, v.imo_number,
        v.flag_state, v.vessel_type, v.is_flagged;

-- ============================================================================
-- SECTION 5: STREAM & TASK MONITORING
-- ============================================================================

USE SCHEMA RAW;

-- Monitoring view for stream status
CREATE OR REPLACE VIEW STREAM_MONITORING AS
SELECT
    name                    AS stream_name,
    table_name              AS source_table,
    type                    AS stream_type,
    stale                   AS is_stale,
    stale_after,
    created_on,
    owner
FROM TABLE(INFORMATION_SCHEMA.STREAMS())
WHERE schema_name = 'RAW'
ORDER BY name;

-- Task execution history (last 24 hours)
CREATE OR REPLACE VIEW TASK_EXECUTION_HISTORY AS
SELECT
    name                    AS task_name,
    state,
    query_start_time,
    completed_time,
    DATEDIFF('second', query_start_time, completed_time) AS duration_seconds,
    error_code,
    error_message,
    return_value
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD(HOUR, -24, CURRENT_TIMESTAMP()),
    RESULT_LIMIT => 100
))
ORDER BY query_start_time DESC;

-- End of 005_streaming_setup.sql
