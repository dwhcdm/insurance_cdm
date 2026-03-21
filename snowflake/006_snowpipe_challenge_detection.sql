-- ============================================================================
-- 006_snowpipe_challenge_detection.sql
-- Detection and correction procedures for enterprise Snowpipe challenges
-- ============================================================================
-- EXECUTION ORDER: Run AFTER 004_snowpipe_setup.sql
--
-- This script creates monitoring views, stored procedures, and alerting
-- infrastructure to DETECT and CORRECT the 20 Snowpipe failure modes
-- that large organisations encounter at scale.
-- ============================================================================

USE ROLE SANCTIONS_DATA_ENGINEER;
USE DATABASE SANCTIONS_DEV;
USE SCHEMA RAW;

-- ============================================================================
-- SECTION 1: COMPREHENSIVE COPY HISTORY MONITORING
-- ============================================================================

-- Unified load-health dashboard across all pipes (last 7 days)
CREATE OR REPLACE VIEW V_SNOWPIPE_LOAD_HEALTH AS
WITH pipe_tables AS (
    SELECT 'PIPE_TRADES' AS pipe_name, 'RAW_TRADES' AS table_name
    UNION ALL SELECT 'PIPE_VESSEL_MOVEMENTS', 'RAW_VESSEL_MOVEMENTS'
    UNION ALL SELECT 'PIPE_SANCTIONS_LISTS', 'RAW_SANCTIONS_LISTS'
    UNION ALL SELECT 'PIPE_COUNTERPARTIES', 'RAW_COUNTERPARTIES'
    UNION ALL SELECT 'PIPE_SCREENING_RESULTS', 'RAW_SCREENING_RESULTS'
    UNION ALL SELECT 'PIPE_VESSELS', 'RAW_VESSELS'
),
copy_hist AS (
    SELECT
        h.pipe_name,
        h.file_name,
        h.stage_location,
        h.row_count,
        h.row_parsed,
        h.file_size,
        h.first_error_message,
        h.first_error_line_num,
        h.error_count,
        h.status,
        h.last_load_time
    FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
        TABLE_NAME => 'RAW_TRADES',
        START_TIME => DATEADD(DAY, -7, CURRENT_TIMESTAMP())
    )) h
    UNION ALL
    SELECT
        h.pipe_name,
        h.file_name,
        h.stage_location,
        h.row_count,
        h.row_parsed,
        h.file_size,
        h.first_error_message,
        h.first_error_line_num,
        h.error_count,
        h.status,
        h.last_load_time
    FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
        TABLE_NAME => 'RAW_VESSEL_MOVEMENTS',
        START_TIME => DATEADD(DAY, -7, CURRENT_TIMESTAMP())
    )) h
    UNION ALL
    SELECT
        h.pipe_name,
        h.file_name,
        h.stage_location,
        h.row_count,
        h.row_parsed,
        h.file_size,
        h.first_error_message,
        h.first_error_line_num,
        h.error_count,
        h.status,
        h.last_load_time
    FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
        TABLE_NAME => 'RAW_SANCTIONS_LISTS',
        START_TIME => DATEADD(DAY, -7, CURRENT_TIMESTAMP())
    )) h
)
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
    status,
    last_load_time,

    -- Derived health indicators
    CASE
        WHEN status = 'LOAD_FAILED' THEN 'FAILED'
        WHEN status = 'PARTIALLY_LOADED' THEN 'PARTIAL'
        WHEN row_count = 0 THEN 'EMPTY_FILE'
        WHEN row_parsed < row_count THEN 'PARTIAL_PARSE'
        WHEN error_count > 0 THEN 'ERRORS_SKIPPED'
        ELSE 'HEALTHY'
    END AS load_health,

    -- Challenge detection flags
    CASE WHEN first_error_message ILIKE '%not a parquet%' THEN TRUE ELSE FALSE END
        AS is_format_mismatch,
    CASE WHEN first_error_message ILIKE '%column%not found%' THEN TRUE ELSE FALSE END
        AS is_schema_drift,
    CASE WHEN row_count = 0 THEN TRUE ELSE FALSE END
        AS is_empty_file,
    CASE WHEN row_parsed > 0 AND row_parsed < row_count THEN TRUE ELSE FALSE END
        AS is_partial_load,
    CASE WHEN first_error_message ILIKE '%numeric value%out of range%'
        THEN TRUE ELSE FALSE END
        AS is_numeric_overflow,
    CASE WHEN first_error_message ILIKE '%timestamp%'
         AND first_error_message ILIKE '%format%' THEN TRUE ELSE FALSE END
        AS is_timestamp_error,
    CASE WHEN first_error_message ILIKE '%access denied%'
         OR first_error_message ILIKE '%authentication%' THEN TRUE ELSE FALSE END
        AS is_permission_error,
    CASE WHEN first_error_message ILIKE '%encoding%'
         OR first_error_message ILIKE '%invalid byte%' THEN TRUE ELSE FALSE END
        AS is_encoding_error

FROM copy_hist
ORDER BY last_load_time DESC;

-- ============================================================================
-- SECTION 2: DUPLICATE DETECTION
-- ============================================================================

-- Detect duplicate trade records loaded from different files
CREATE OR REPLACE VIEW V_DUPLICATE_TRADES AS
SELECT
    trade_id,
    COUNT(*) AS duplicate_count,
    COUNT(DISTINCT _loaded_at) AS distinct_load_times,
    MIN(_loaded_at) AS first_loaded,
    MAX(_loaded_at) AS last_loaded,
    LISTAGG(DISTINCT source_system, ', ') AS source_systems
FROM RAW_TRADES
GROUP BY trade_id
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;

-- Detect duplicate vessel records
CREATE OR REPLACE VIEW V_DUPLICATE_VESSELS AS
SELECT
    vessel_id,
    imo_number,
    COUNT(*) AS duplicate_count,
    COUNT(DISTINCT _loaded_at) AS distinct_load_times,
    MIN(_loaded_at) AS first_loaded,
    MAX(_loaded_at) AS last_loaded
FROM RAW_VESSELS
GROUP BY vessel_id, imo_number
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;

-- ============================================================================
-- SECTION 3: PIPE STATUS MONITORING
-- ============================================================================

-- Comprehensive pipe health check (detects Challenge 9: Stale Pipes)
CREATE OR REPLACE PROCEDURE SP_CHECK_PIPE_HEALTH()
RETURNS TABLE (
    pipe_name VARCHAR,
    pipe_status VARCHAR,
    is_healthy BOOLEAN,
    issue_detected VARCHAR,
    recommended_action VARCHAR
)
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    res RESULTSET DEFAULT (
        SELECT
            'PIPE_TRADES' AS pipe_name,
            SYSTEM$PIPE_STATUS('PIPE_TRADES') AS pipe_status,
            CASE
                WHEN SYSTEM$PIPE_STATUS('PIPE_TRADES') ILIKE '%RUNNING%' THEN TRUE
                ELSE FALSE
            END AS is_healthy,
            CASE
                WHEN SYSTEM$PIPE_STATUS('PIPE_TRADES') ILIKE '%STALE%'
                    THEN 'PIPE_STALE - DDL change detected'
                WHEN SYSTEM$PIPE_STATUS('PIPE_TRADES') ILIKE '%PAUSED%'
                    THEN 'PIPE_PAUSED - Manual intervention needed'
                ELSE 'OK'
            END AS issue_detected,
            CASE
                WHEN SYSTEM$PIPE_STATUS('PIPE_TRADES') ILIKE '%STALE%'
                    THEN 'CREATE OR REPLACE PIPE with same definition, then ALTER PIPE REFRESH'
                WHEN SYSTEM$PIPE_STATUS('PIPE_TRADES') ILIKE '%PAUSED%'
                    THEN 'ALTER PIPE PIPE_TRADES SET PIPE_EXECUTION_PAUSED = FALSE'
                ELSE 'No action required'
            END AS recommended_action
        UNION ALL
        SELECT
            'PIPE_VESSEL_MOVEMENTS',
            SYSTEM$PIPE_STATUS('PIPE_VESSEL_MOVEMENTS'),
            CASE WHEN SYSTEM$PIPE_STATUS('PIPE_VESSEL_MOVEMENTS') ILIKE '%RUNNING%'
                THEN TRUE ELSE FALSE END,
            CASE
                WHEN SYSTEM$PIPE_STATUS('PIPE_VESSEL_MOVEMENTS') ILIKE '%STALE%'
                    THEN 'PIPE_STALE - DDL change detected'
                WHEN SYSTEM$PIPE_STATUS('PIPE_VESSEL_MOVEMENTS') ILIKE '%PAUSED%'
                    THEN 'PIPE_PAUSED - Manual intervention needed'
                ELSE 'OK'
            END,
            CASE
                WHEN SYSTEM$PIPE_STATUS('PIPE_VESSEL_MOVEMENTS') ILIKE '%STALE%'
                    THEN 'CREATE OR REPLACE PIPE, then ALTER PIPE REFRESH'
                WHEN SYSTEM$PIPE_STATUS('PIPE_VESSEL_MOVEMENTS') ILIKE '%PAUSED%'
                    THEN 'ALTER PIPE SET PIPE_EXECUTION_PAUSED = FALSE'
                ELSE 'No action required'
            END
        UNION ALL
        SELECT
            'PIPE_SANCTIONS_LISTS',
            SYSTEM$PIPE_STATUS('PIPE_SANCTIONS_LISTS'),
            CASE WHEN SYSTEM$PIPE_STATUS('PIPE_SANCTIONS_LISTS') ILIKE '%RUNNING%'
                THEN TRUE ELSE FALSE END,
            CASE
                WHEN SYSTEM$PIPE_STATUS('PIPE_SANCTIONS_LISTS') ILIKE '%STALE%'
                    THEN 'PIPE_STALE - CRITICAL: Regulatory data pipe is stale'
                WHEN SYSTEM$PIPE_STATUS('PIPE_SANCTIONS_LISTS') ILIKE '%PAUSED%'
                    THEN 'PIPE_PAUSED - CRITICAL: Regulatory data pipe is paused'
                ELSE 'OK'
            END,
            CASE
                WHEN SYSTEM$PIPE_STATUS('PIPE_SANCTIONS_LISTS') ILIKE '%STALE%'
                    THEN 'URGENT: Recreate pipe and refresh immediately'
                WHEN SYSTEM$PIPE_STATUS('PIPE_SANCTIONS_LISTS') ILIKE '%PAUSED%'
                    THEN 'URGENT: Resume pipe immediately'
                ELSE 'No action required'
            END
        UNION ALL
        SELECT
            'PIPE_COUNTERPARTIES',
            SYSTEM$PIPE_STATUS('PIPE_COUNTERPARTIES'),
            CASE WHEN SYSTEM$PIPE_STATUS('PIPE_COUNTERPARTIES') ILIKE '%RUNNING%'
                THEN TRUE ELSE FALSE END,
            CASE
                WHEN SYSTEM$PIPE_STATUS('PIPE_COUNTERPARTIES') ILIKE '%STALE%'
                    THEN 'PIPE_STALE - DDL change detected'
                ELSE 'OK'
            END,
            CASE
                WHEN SYSTEM$PIPE_STATUS('PIPE_COUNTERPARTIES') ILIKE '%STALE%'
                    THEN 'Recreate pipe and refresh'
                ELSE 'No action required'
            END
        UNION ALL
        SELECT
            'PIPE_SCREENING_RESULTS',
            SYSTEM$PIPE_STATUS('PIPE_SCREENING_RESULTS'),
            CASE WHEN SYSTEM$PIPE_STATUS('PIPE_SCREENING_RESULTS') ILIKE '%RUNNING%'
                THEN TRUE ELSE FALSE END,
            CASE
                WHEN SYSTEM$PIPE_STATUS('PIPE_SCREENING_RESULTS') ILIKE '%STALE%'
                    THEN 'PIPE_STALE - DDL change detected'
                ELSE 'OK'
            END,
            CASE
                WHEN SYSTEM$PIPE_STATUS('PIPE_SCREENING_RESULTS') ILIKE '%STALE%'
                    THEN 'Recreate pipe and refresh'
                ELSE 'No action required'
            END
        UNION ALL
        SELECT
            'PIPE_VESSELS',
            SYSTEM$PIPE_STATUS('PIPE_VESSELS'),
            CASE WHEN SYSTEM$PIPE_STATUS('PIPE_VESSELS') ILIKE '%RUNNING%'
                THEN TRUE ELSE FALSE END,
            CASE
                WHEN SYSTEM$PIPE_STATUS('PIPE_VESSELS') ILIKE '%STALE%'
                    THEN 'PIPE_STALE - DDL change detected'
                ELSE 'OK'
            END,
            CASE
                WHEN SYSTEM$PIPE_STATUS('PIPE_VESSELS') ILIKE '%STALE%'
                    THEN 'Recreate pipe and refresh'
                ELSE 'No action required'
            END
    );
BEGIN
    RETURN TABLE(res);
END;
$$;

-- ============================================================================
-- SECTION 4: LOAD LATENCY / SLA MONITORING
-- ============================================================================

-- Track ingestion latency per pipe (Challenge 13)
CREATE OR REPLACE VIEW V_LOAD_LATENCY_MONITOR AS
SELECT
    pipe_name,
    file_name,
    last_load_time,
    -- Approximate stage time from filename pattern (assumes YYYYMMDD_HHMMSS)
    -- In production, compare against S3 event timestamp
    DATEDIFF(
        'minute',
        DATEADD('hour', -1, last_load_time),  -- Approximate stage time
        last_load_time
    ) AS estimated_latency_minutes,
    CASE
        WHEN DATEDIFF('minute',
            DATEADD('hour', -1, last_load_time), last_load_time) > 240
            THEN 'CRITICAL_SLA_BREACH'
        WHEN DATEDIFF('minute',
            DATEADD('hour', -1, last_load_time), last_load_time) > 60
            THEN 'SLA_WARNING'
        WHEN DATEDIFF('minute',
            DATEADD('hour', -1, last_load_time), last_load_time) > 30
            THEN 'ELEVATED'
        ELSE 'NORMAL'
    END AS latency_tier,
    row_count,
    file_size,
    status
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'RAW_TRADES',
    START_TIME => DATEADD(DAY, -1, CURRENT_TIMESTAMP())
))
WHERE status = 'LOADED'
ORDER BY last_load_time DESC;

-- Hourly load volume trend (detects volume spikes/gaps — Challenge 6/8)
CREATE OR REPLACE VIEW V_HOURLY_LOAD_VOLUME AS
SELECT
    DATE_TRUNC('hour', last_load_time) AS load_hour,
    pipe_name,
    COUNT(*) AS files_loaded,
    SUM(row_count) AS total_rows,
    SUM(file_size) AS total_bytes,
    AVG(file_size) AS avg_file_size,
    MAX(file_size) AS max_file_size,
    SUM(error_count) AS total_errors,
    COUNT_IF(status = 'LOAD_FAILED') AS failed_files,
    COUNT_IF(row_count = 0) AS empty_files
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'RAW_TRADES',
    START_TIME => DATEADD(DAY, -7, CURRENT_TIMESTAMP())
))
GROUP BY load_hour, pipe_name
ORDER BY load_hour DESC;

-- ============================================================================
-- SECTION 5: CORRECTION PROCEDURES
-- ============================================================================

-- Procedure: Deduplicate trades after duplicate file load (Challenge 4/17)
CREATE OR REPLACE PROCEDURE SP_DEDUP_TRADES()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    dupe_count INTEGER;
    delete_count INTEGER;
BEGIN
    -- Count duplicates before dedup
    SELECT COUNT(*) INTO :dupe_count
    FROM (
        SELECT trade_id
        FROM RAW_TRADES
        GROUP BY trade_id
        HAVING COUNT(*) > 1
    );

    IF (dupe_count = 0) THEN
        RETURN 'No duplicates found. Table is clean.';
    END IF;

    -- Create temp table with deduplicated records
    CREATE OR REPLACE TEMPORARY TABLE TMP_TRADES_DEDUP AS
    SELECT *
    FROM RAW_TRADES
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY trade_id
        ORDER BY _loaded_at DESC
    ) = 1;

    -- Swap: delete dupes and re-insert clean data
    DELETE FROM RAW_TRADES
    WHERE trade_id IN (
        SELECT trade_id
        FROM RAW_TRADES
        GROUP BY trade_id
        HAVING COUNT(*) > 1
    );

    INSERT INTO RAW_TRADES
    SELECT * FROM TMP_TRADES_DEDUP
    WHERE trade_id NOT IN (SELECT trade_id FROM RAW_TRADES);

    DROP TABLE IF EXISTS TMP_TRADES_DEDUP;

    RETURN 'Deduplication complete. Resolved ' || :dupe_count || ' duplicate trade_id groups.';
END;
$$;

-- Procedure: Refresh all stale pipes (Challenge 9)
CREATE OR REPLACE PROCEDURE SP_REFRESH_STALE_PIPES()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    refreshed_count INTEGER DEFAULT 0;
    pipe_status_trades VARCHAR;
    pipe_status_vessels VARCHAR;
    pipe_status_sanctions VARCHAR;
BEGIN
    -- Check each pipe and refresh if stale
    SELECT SYSTEM$PIPE_STATUS('PIPE_TRADES') INTO :pipe_status_trades;
    IF (pipe_status_trades ILIKE '%STALE%') THEN
        ALTER PIPE PIPE_TRADES REFRESH;
        refreshed_count := refreshed_count + 1;
    END IF;

    SELECT SYSTEM$PIPE_STATUS('PIPE_VESSEL_MOVEMENTS') INTO :pipe_status_vessels;
    IF (pipe_status_vessels ILIKE '%STALE%') THEN
        ALTER PIPE PIPE_VESSEL_MOVEMENTS REFRESH;
        refreshed_count := refreshed_count + 1;
    END IF;

    SELECT SYSTEM$PIPE_STATUS('PIPE_SANCTIONS_LISTS') INTO :pipe_status_sanctions;
    IF (pipe_status_sanctions ILIKE '%STALE%') THEN
        ALTER PIPE PIPE_SANCTIONS_LISTS REFRESH;
        refreshed_count := refreshed_count + 1;
    END IF;

    RETURN 'Refreshed ' || :refreshed_count || ' stale pipe(s).';
END;
$$;

-- Procedure: Reconcile stage files vs loaded files (Challenge 8)
CREATE OR REPLACE PROCEDURE SP_RECONCILE_STAGE_VS_LOADED(
    stage_name VARCHAR,
    target_table VARCHAR
)
RETURNS TABLE (
    file_name VARCHAR,
    in_stage BOOLEAN,
    in_copy_history BOOLEAN,
    status VARCHAR,
    recommended_action VARCHAR
)
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    res RESULTSET DEFAULT (
        WITH staged_files AS (
            SELECT
                "name" AS file_name,
                "size" AS file_size,
                "last_modified" AS staged_at
            FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
        ),
        loaded_files AS (
            SELECT DISTINCT
                file_name,
                status,
                last_load_time
            FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
                TABLE_NAME => target_table,
                START_TIME => DATEADD(DAY, -14, CURRENT_TIMESTAMP())
            ))
        )
        SELECT
            COALESCE(s.file_name, l.file_name) AS file_name,
            s.file_name IS NOT NULL AS in_stage,
            l.file_name IS NOT NULL AS in_copy_history,
            COALESCE(l.status, 'NOT_LOADED') AS status,
            CASE
                WHEN s.file_name IS NOT NULL AND l.file_name IS NULL
                    THEN 'ALTER PIPE REFRESH to process missed file'
                WHEN s.file_name IS NULL AND l.file_name IS NOT NULL
                    AND l.status = 'LOAD_FAILED'
                    THEN 'Re-stage file and retry'
                WHEN l.status = 'LOAD_FAILED'
                    THEN 'Investigate error and re-load'
                ELSE 'No action required'
            END AS recommended_action
        FROM staged_files s
        FULL OUTER JOIN loaded_files l ON s.file_name = l.file_name
        ORDER BY file_name
    );
BEGIN
    RETURN TABLE(res);
END;
$$;

-- Procedure: Quarantine bad rows from ON_ERROR=CONTINUE loads (Challenge 10)
CREATE OR REPLACE PROCEDURE SP_QUARANTINE_BAD_ROWS()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    quarantined_count INTEGER DEFAULT 0;
BEGIN
    -- Create quarantine table if not exists
    CREATE TABLE IF NOT EXISTS RAW.QUARANTINE_TRADES (
        trade_id                VARCHAR(20),
        original_data           VARIANT,
        error_type              VARCHAR(100),
        error_detail            VARCHAR(4000),
        quarantined_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        resolved                BOOLEAN DEFAULT FALSE,
        resolved_at             TIMESTAMP_NTZ,
        resolved_by             VARCHAR(50)
    );

    -- Move trades with impossible values to quarantine
    INSERT INTO RAW.QUARANTINE_TRADES (trade_id, original_data, error_type, error_detail)
    SELECT
        trade_id,
        OBJECT_CONSTRUCT(*),
        CASE
            WHEN total_value_usd < 0 THEN 'NEGATIVE_VALUE'
            WHEN total_value_usd > 9999999999999999.99 THEN 'NUMERIC_OVERFLOW'
            WHEN sanctions_risk_score > 100 OR sanctions_risk_score < 0
                THEN 'IMPOSSIBLE_SCORE'
            WHEN trade_date > CURRENT_DATE() + 30 THEN 'FUTURE_DATED'
            WHEN trade_date < '1990-01-01' THEN 'HISTORICAL_OUTLIER'
            ELSE 'BUSINESS_RULE_VIOLATION'
        END,
        CASE
            WHEN total_value_usd < 0
                THEN 'Negative trade value: ' || total_value_usd::VARCHAR
            WHEN total_value_usd > 9999999999999999.99
                THEN 'Value exceeds column precision: ' || total_value_usd::VARCHAR
            WHEN sanctions_risk_score > 100
                THEN 'Score exceeds maximum: ' || sanctions_risk_score::VARCHAR
            WHEN trade_date > CURRENT_DATE() + 30
                THEN 'Trade date too far in future: ' || trade_date::VARCHAR
            WHEN trade_date < '1990-01-01'
                THEN 'Trade date before cutoff: ' || trade_date::VARCHAR
            ELSE 'Unknown violation'
        END
    FROM RAW_TRADES
    WHERE total_value_usd < 0
       OR total_value_usd > 9999999999999999.99
       OR sanctions_risk_score > 100
       OR sanctions_risk_score < 0
       OR trade_date > CURRENT_DATE() + 30
       OR trade_date < '1990-01-01';

    SELECT COUNT(*) INTO :quarantined_count
    FROM RAW.QUARANTINE_TRADES
    WHERE quarantined_at >= DATEADD('minute', -5, CURRENT_TIMESTAMP());

    -- Remove quarantined rows from production table
    DELETE FROM RAW_TRADES
    WHERE trade_id IN (
        SELECT trade_id
        FROM RAW.QUARANTINE_TRADES
        WHERE resolved = FALSE
    );

    RETURN 'Quarantined ' || :quarantined_count || ' bad row(s) from RAW_TRADES.';
END;
$$;

-- ============================================================================
-- SECTION 6: ORPHAN FK DETECTION (Challenge 11)
-- ============================================================================

-- Trades referencing non-existent counterparties
CREATE OR REPLACE VIEW V_ORPHAN_TRADE_COUNTERPARTIES AS
SELECT
    t.trade_id,
    t.buyer_counterparty_id,
    t.seller_counterparty_id,
    CASE
        WHEN bc.counterparty_id IS NULL AND sc.counterparty_id IS NULL
            THEN 'BOTH_ORPHANED'
        WHEN bc.counterparty_id IS NULL
            THEN 'BUYER_ORPHANED'
        WHEN sc.counterparty_id IS NULL
            THEN 'SELLER_ORPHANED'
        ELSE 'OK'
    END AS orphan_status,
    t._loaded_at AS trade_loaded_at
FROM RAW_TRADES t
LEFT JOIN RAW_COUNTERPARTIES bc
    ON t.buyer_counterparty_id = bc.counterparty_id
LEFT JOIN RAW_COUNTERPARTIES sc
    ON t.seller_counterparty_id = sc.counterparty_id
WHERE bc.counterparty_id IS NULL
   OR sc.counterparty_id IS NULL;

-- Trades referencing non-existent vessels
CREATE OR REPLACE VIEW V_ORPHAN_TRADE_VESSELS AS
SELECT
    t.trade_id,
    t.vessel_id,
    t._loaded_at AS trade_loaded_at
FROM RAW_TRADES t
LEFT JOIN RAW_VESSELS v
    ON t.vessel_id = v.vessel_id
WHERE t.vessel_id IS NOT NULL
  AND v.vessel_id IS NULL;

-- ============================================================================
-- SECTION 7: ENCODING & DATA QUALITY DETECTION (Challenge 7/15)
-- ============================================================================

-- Detect encoding issues in text columns
CREATE OR REPLACE VIEW V_ENCODING_ISSUES AS
SELECT
    'RAW_COUNTERPARTIES' AS table_name,
    counterparty_id AS record_id,
    'legal_name' AS column_name,
    legal_name AS value,
    CASE
        WHEN legal_name LIKE '%\xEF\xBF\xBD%' THEN 'REPLACEMENT_CHARACTER'
        WHEN legal_name LIKE '%\x00%' THEN 'NULL_BYTE'
        WHEN legal_name REGEXP '[\x80-\xFF]{3,}' THEN 'POSSIBLE_MOJIBAKE'
        WHEN legal_name LIKE '%\xEF\xBB\xBF%' THEN 'BOM_IN_DATA'
        ELSE 'SUSPICIOUS_ENCODING'
    END AS issue_type
FROM RAW_COUNTERPARTIES
WHERE legal_name LIKE '%\xEF\xBF\xBD%'
   OR legal_name LIKE '%\x00%'
   OR legal_name LIKE '%\xEF\xBB\xBF%'
   OR legal_name REGEXP '[\x80-\xFF]{3,}';

-- ============================================================================
-- SECTION 8: FILE SIZE ANOMALY DETECTION (Challenge 6)
-- ============================================================================

CREATE OR REPLACE VIEW V_FILE_SIZE_ANOMALIES AS
WITH file_stats AS (
    SELECT
        pipe_name,
        AVG(file_size) AS avg_file_size,
        STDDEV(file_size) AS stddev_file_size
    FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
        TABLE_NAME => 'RAW_TRADES',
        START_TIME => DATEADD(DAY, -7, CURRENT_TIMESTAMP())
    ))
    WHERE status = 'LOADED'
    GROUP BY pipe_name
)
SELECT
    h.pipe_name,
    h.file_name,
    h.file_size,
    h.row_count,
    s.avg_file_size,
    ROUND(h.file_size / NULLIF(s.avg_file_size, 0), 2) AS size_ratio,
    CASE
        WHEN h.file_size > s.avg_file_size + 3 * s.stddev_file_size
            THEN 'OVERSIZED (>3 sigma)'
        WHEN h.file_size > s.avg_file_size * 5
            THEN 'OVERSIZED (>5x avg)'
        WHEN h.file_size < s.avg_file_size * 0.1
            AND h.file_size > 0
            THEN 'UNDERSIZED (<10% avg)'
        WHEN h.file_size = 0
            THEN 'EMPTY_FILE'
        ELSE 'NORMAL'
    END AS size_anomaly
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'RAW_TRADES',
    START_TIME => DATEADD(DAY, -7, CURRENT_TIMESTAMP())
)) h
JOIN file_stats s ON h.pipe_name = s.pipe_name
WHERE h.file_size > s.avg_file_size * 5
   OR h.file_size < s.avg_file_size * 0.1
   OR h.file_size = 0
ORDER BY h.last_load_time DESC;

-- ============================================================================
-- SECTION 9: COMPREHENSIVE HEALTH REPORT PROCEDURE
-- ============================================================================

-- Master health check that runs all detection queries
CREATE OR REPLACE PROCEDURE SP_SNOWPIPE_HEALTH_REPORT()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    report VARCHAR DEFAULT '';
    dupe_count INTEGER DEFAULT 0;
    orphan_count INTEGER DEFAULT 0;
    failed_loads INTEGER DEFAULT 0;
    empty_loads INTEGER DEFAULT 0;
BEGIN
    -- 1. Check for duplicates
    SELECT COUNT(*) INTO :dupe_count
    FROM (
        SELECT trade_id FROM RAW_TRADES
        GROUP BY trade_id HAVING COUNT(*) > 1
    );
    report := report || 'Duplicate trade_ids: ' || :dupe_count || '\n';

    -- 2. Check for orphan FKs
    SELECT COUNT(*) INTO :orphan_count
    FROM RAW_TRADES t
    LEFT JOIN RAW_COUNTERPARTIES c ON t.buyer_counterparty_id = c.counterparty_id
    WHERE c.counterparty_id IS NULL;
    report := report || 'Orphan buyer FKs: ' || :orphan_count || '\n';

    -- 3. Check for failed loads (last 24h)
    SELECT COUNT(*) INTO :failed_loads
    FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
        TABLE_NAME => 'RAW_TRADES',
        START_TIME => DATEADD(DAY, -1, CURRENT_TIMESTAMP())
    ))
    WHERE status = 'LOAD_FAILED';
    report := report || 'Failed loads (24h): ' || :failed_loads || '\n';

    -- 4. Check for empty loads
    SELECT COUNT(*) INTO :empty_loads
    FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
        TABLE_NAME => 'RAW_TRADES',
        START_TIME => DATEADD(DAY, -1, CURRENT_TIMESTAMP())
    ))
    WHERE row_count = 0;
    report := report || 'Empty file loads (24h): ' || :empty_loads || '\n';

    -- Summary
    IF (dupe_count > 0 OR orphan_count > 0 OR failed_loads > 0) THEN
        report := report || '\nACTION REQUIRED: Run SP_DEDUP_TRADES, '
                         || 'SP_REFRESH_STALE_PIPES, SP_QUARANTINE_BAD_ROWS '
                         || 'as needed.';
    ELSE
        report := report || '\nAll checks passed. Snowpipe health is GOOD.';
    END IF;

    RETURN report;
END;
$$;

-- ============================================================================
-- SECTION 10: AUTOMATED CORRECTION TASK
-- ============================================================================

-- Scheduled task to run health checks and auto-correct common issues
CREATE OR REPLACE TASK TASK_SNOWPIPE_HEALTH_CHECK
    WAREHOUSE = SANCTIONS_TRANSFORM_WH_XS
    SCHEDULE = '30 MINUTE'
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    COMMENT = 'Automated Snowpipe health check and correction every 30 minutes'
AS
    CALL SP_SNOWPIPE_HEALTH_REPORT();

-- NOTE: Task is created in SUSPENDED state. To activate:
-- ALTER TASK TASK_SNOWPIPE_HEALTH_CHECK RESUME;

-- End of 006_snowpipe_challenge_detection.sql
