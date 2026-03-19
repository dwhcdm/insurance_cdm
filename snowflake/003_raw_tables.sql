-- ============================================================================
-- SCRIPT: 003_raw_tables.sql
-- PURPOSE: Create raw/landing tables for all data domains
-- RUN AS:  SANCTIONS_PLATFORM_ADMIN or SANCTIONS_DATA_ENGINEER
-- VERSION: 1.0.0
--
-- DESIGN DECISIONS:
--   - TRANSIENT tables in RAW: No fail-safe (cost optimization)
--   - 1-day retention: Minimal Time Travel (data is reproducible)
--   - Metadata columns: _loaded_at, _file_name, _file_row_number
--   - Governance tags: Applied for lineage and classification
--   - CLUSTER BY on high-volume tables for query optimization
-- ============================================================================

USE ROLE SANCTIONS_PLATFORM_ADMIN;
USE DATABASE SANCTIONS_DEV;
USE SCHEMA RAW;

-- ============================================================================
-- 1. RAW_COUNTERPARTIES (5M records)
-- ============================================================================

CREATE OR REPLACE TRANSIENT TABLE RAW.RAW_COUNTERPARTIES (
    counterparty_id             VARCHAR(20)     NOT NULL,
    legal_name                  VARCHAR(500)    NOT NULL,
    short_name                  VARCHAR(200),
    entity_type                 VARCHAR(50),      -- CORPORATE, INDIVIDUAL, GOVERNMENT, BANK, TRADER
    country_of_incorporation    VARCHAR(3)      NOT NULL,
    country_of_domicile         VARCHAR(3),
    sector                      VARCHAR(100),
    is_pep                      BOOLEAN         DEFAULT FALSE,
    pep_level                   VARCHAR(20),      -- DIRECT, FAMILY, ASSOCIATE
    risk_rating                 VARCHAR(10)     NOT NULL,   -- HIGH, MEDIUM, LOW
    registration_number         VARCHAR(100),
    lei_code                    VARCHAR(20),       -- Legal Entity Identifier
    swift_bic                   VARCHAR(11),
    alias_names                 VARIANT,           -- JSON array of aliases
    registration_date           DATE,
    last_kyc_date               DATE,
    next_kyc_due_date           DATE,
    kyc_status                  VARCHAR(20),      -- ACTIVE, EXPIRED, PENDING, SUSPENDED
    is_active                   BOOLEAN         DEFAULT TRUE,
    source_system               VARCHAR(50),
    created_at                  TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    updated_at                  TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),

    -- Metadata columns (populated during COPY INTO)
    _loaded_at                  TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    _file_name                  VARCHAR(500),
    _file_row_number            NUMBER
)
DATA_RETENTION_TIME_IN_DAYS = 1
COMMENT = 'Raw counterparty/entity data from KYC systems'
;

-- Apply governance tags
ALTER TABLE RAW.RAW_COUNTERPARTIES SET TAG
    GOVERNANCE.TAGS.DATA_DOMAIN = 'COUNTERPARTY',
    GOVERNANCE.TAGS.DATA_CLASSIFICATION = 'CONFIDENTIAL';

ALTER TABLE RAW.RAW_COUNTERPARTIES MODIFY COLUMN legal_name SET TAG
    GOVERNANCE.TAGS.PII_TYPE = 'NAME';
ALTER TABLE RAW.RAW_COUNTERPARTIES MODIFY COLUMN lei_code SET TAG
    GOVERNANCE.TAGS.PII_TYPE = 'ID_NUMBER';
ALTER TABLE RAW.RAW_COUNTERPARTIES MODIFY COLUMN swift_bic SET TAG
    GOVERNANCE.TAGS.PII_TYPE = 'FINANCIAL';


-- ============================================================================
-- 2. RAW_SANCTIONS_LISTS (250K records)
-- ============================================================================

CREATE OR REPLACE TRANSIENT TABLE RAW.RAW_SANCTIONS_LISTS (
    entity_id                   VARCHAR(50)     NOT NULL,
    entity_name                 VARCHAR(500)    NOT NULL,
    entity_type                 VARCHAR(50),
    sanctions_list              VARCHAR(50)     NOT NULL,   -- OFAC_SDN, EU_SANCTIONS, UN_SANCTIONS, etc.
    sanctions_program           VARCHAR(200),
    listed_date                 DATE,
    delisted_date               DATE,
    country_codes               VARIANT,           -- JSON array of country codes
    alias_names                 VARIANT,           -- JSON array of aliases
    identification_numbers      VARIANT,           -- JSON array of IDs
    date_of_birth               VARCHAR(50),       -- VARCHAR to handle partial dates
    nationality                 VARCHAR(3),
    remarks                     TEXT,
    is_active                   BOOLEAN         DEFAULT TRUE,
    source_url                  VARCHAR(2000),
    last_updated                TIMESTAMP_NTZ,
    created_at                  TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),

    -- Metadata columns
    _loaded_at                  TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    _file_name                  VARCHAR(500),
    _file_row_number            NUMBER
)
DATA_RETENTION_TIME_IN_DAYS = 1
COMMENT = 'Raw sanctions list data from OFAC/EU/UN/UK sources'
;

ALTER TABLE RAW.RAW_SANCTIONS_LISTS SET TAG
    GOVERNANCE.TAGS.DATA_DOMAIN = 'SANCTIONS',
    GOVERNANCE.TAGS.DATA_CLASSIFICATION = 'SANCTIONS_SENSITIVE';


-- ============================================================================
-- 3. RAW_VESSELS (500K records)
-- ============================================================================

CREATE OR REPLACE TRANSIENT TABLE RAW.RAW_VESSELS (
    vessel_id                   VARCHAR(20)     NOT NULL,
    imo_number                  VARCHAR(10)     NOT NULL,   -- IMO unique identifier
    mmsi_number                 VARCHAR(15),
    vessel_name                 VARCHAR(200)    NOT NULL,
    vessel_type                 VARCHAR(50),      -- VLCC, SUEZMAX, AFRAMAX, PANAMAX, etc.
    flag_state                  VARCHAR(3),       -- ISO country code
    dwt_tonnage                 NUMBER(12,2),
    gross_tonnage               NUMBER(12,2),
    build_year                  NUMBER(4,0),
    builder                     VARCHAR(200),
    owner_name                  VARCHAR(200),
    operator_name               VARCHAR(200),
    class_society               VARCHAR(50),      -- DNV, LLOYD, BV, ABS, etc.
    call_sign                   VARCHAR(10),
    is_flagged                  BOOLEAN         DEFAULT FALSE,
    flag_reason                 VARCHAR(200),     -- Reason for flagging
    last_inspection_date        DATE,
    is_active                   BOOLEAN         DEFAULT TRUE,
    source_system               VARCHAR(50),
    created_at                  TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    updated_at                  TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),

    -- Metadata columns
    _loaded_at                  TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    _file_name                  VARCHAR(500),
    _file_row_number            NUMBER
)
DATA_RETENTION_TIME_IN_DAYS = 1
COMMENT = 'Raw vessel master data from Lloyd''s, Equasis, AIS providers'
;

ALTER TABLE RAW.RAW_VESSELS SET TAG
    GOVERNANCE.TAGS.DATA_DOMAIN = 'VESSEL',
    GOVERNANCE.TAGS.DATA_CLASSIFICATION = 'INTERNAL';


-- ============================================================================
-- 4. RAW_TRADES (912.5M records - THE BIG TABLE)
-- ============================================================================

CREATE OR REPLACE TRANSIENT TABLE RAW.RAW_TRADES (
    trade_id                    VARCHAR(20)     NOT NULL,
    trade_reference             VARCHAR(50),
    trade_timestamp             TIMESTAMP_NTZ   NOT NULL,
    trade_date                  DATE            NOT NULL,
    settlement_date             DATE,
    trade_type                  VARCHAR(20),      -- PHYSICAL, PAPER, SWAP, OPTION, FUTURE
    trade_status                VARCHAR(20),      -- CONFIRMED, PENDING, SETTLED, CANCELLED

    -- Counterparty references
    buyer_counterparty_id       VARCHAR(20),
    seller_counterparty_id      VARCHAR(20),

    -- Commodity details
    commodity_code              VARCHAR(20),
    commodity_name              VARCHAR(200),
    commodity_group             VARCHAR(50),      -- CRUDE_OIL, REFINED_PRODUCTS, LNG, METALS, etc.

    -- Financial details
    quantity_mt                 NUMBER(18,4),     -- Metric tonnes
    price_per_mt_usd            NUMBER(18,4),
    total_value_usd             NUMBER(18,2)    NOT NULL,
    currency                    VARCHAR(3)      DEFAULT 'USD',
    fx_rate                     NUMBER(18,8)    DEFAULT 1.0,
    incoterm                    VARCHAR(5),       -- FOB, CIF, CFR, DES, DAP

    -- Geography
    origin_country              VARCHAR(3),
    destination_country         VARCHAR(3),
    loading_port                VARCHAR(100),
    discharge_port              VARCHAR(100),

    -- Vessel
    vessel_id                   VARCHAR(20),
    vessel_flagged              BOOLEAN         DEFAULT FALSE,

    -- Risk indicators
    sanctions_risk_score        NUMBER(5,2),      -- 0-100 composite score
    screening_status            VARCHAR(30),      -- AUTO_CLEARED, REVIEW_REQUIRED, ESCALATED

    -- Operational
    booking_entity              VARCHAR(100),
    trader_id                   VARCHAR(20),
    desk                        VARCHAR(50),
    source_system               VARCHAR(50),

    -- Timestamps
    created_at                  TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    updated_at                  TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),

    -- Metadata columns
    _loaded_at                  TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    _file_name                  VARCHAR(500),
    _file_row_number            NUMBER
)
DATA_RETENTION_TIME_IN_DAYS = 1
CLUSTER BY (trade_date)
COMMENT = 'Raw trade transactions - BILLIONS scale, clustered by trade_date'
;

ALTER TABLE RAW.RAW_TRADES SET TAG
    GOVERNANCE.TAGS.DATA_DOMAIN = 'TRADE',
    GOVERNANCE.TAGS.DATA_CLASSIFICATION = 'CONFIDENTIAL';


-- ============================================================================
-- 5. RAW_VESSEL_MOVEMENTS (3.65B records - AIS data, BILLIONS scale)
-- ============================================================================

CREATE OR REPLACE TRANSIENT TABLE RAW.RAW_VESSEL_MOVEMENTS (
    movement_id                 VARCHAR(20)     NOT NULL,
    vessel_id                   VARCHAR(20)     NOT NULL,
    timestamp                   TIMESTAMP_NTZ   NOT NULL,
    latitude                    NUMBER(10,6)    NOT NULL,
    longitude                   NUMBER(10,6)    NOT NULL,
    speed_knots                 NUMBER(5,1),
    heading                     NUMBER(5,1),
    port_of_call                VARCHAR(100),
    origin_port                 VARCHAR(100),
    destination_port            VARCHAR(100),
    voyage_id                   VARCHAR(100),
    is_dark_activity            BOOLEAN         DEFAULT FALSE,
    dark_duration_hours         NUMBER(8,1),
    zone_risk_score             NUMBER(5,2)     DEFAULT 0,
    is_near_sanctioned_zone     BOOLEAN         DEFAULT FALSE,
    ais_message_type            NUMBER(2,0),
    navigation_status           VARCHAR(50),
    source_system               VARCHAR(50),
    created_at                  TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),

    -- Metadata columns
    _loaded_at                  TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    _file_name                  VARCHAR(500),
    _file_row_number            NUMBER
)
DATA_RETENTION_TIME_IN_DAYS = 1
CLUSTER BY (timestamp::DATE, vessel_id)
COMMENT = 'Raw AIS vessel movement data - BILLIONS scale, clustered by date and vessel'
;

ALTER TABLE RAW.RAW_VESSEL_MOVEMENTS SET TAG
    GOVERNANCE.TAGS.DATA_DOMAIN = 'VESSEL',
    GOVERNANCE.TAGS.DATA_CLASSIFICATION = 'INTERNAL';


-- ============================================================================
-- 6. RAW_SCREENING_RESULTS (136.9M records)
-- ============================================================================

CREATE OR REPLACE TRANSIENT TABLE RAW.RAW_SCREENING_RESULTS (
    screening_id                VARCHAR(20)     NOT NULL,
    trade_id                    VARCHAR(20)     NOT NULL,
    counterparty_id             VARCHAR(20),
    sanctioned_entity_id        VARCHAR(20),
    screening_timestamp         TIMESTAMP_NTZ   NOT NULL,
    screening_type              VARCHAR(30),      -- PRE_TRADE, POST_TRADE, PERIODIC, EVENT_DRIVEN, RETROSPECTIVE
    match_score                 NUMBER(6,4),      -- 0.0000 to 1.0000
    match_type                  VARCHAR(30),      -- NAME_MATCH, FUZZY_NAME, ID_MATCH, etc.
    match_details               VARIANT,           -- JSON with match algorithm details
    disposition                 VARCHAR(30),      -- TRUE_POSITIVE, FALSE_POSITIVE, ESCALATED, etc.
    risk_level                  VARCHAR(10),      -- HIGH, MEDIUM, LOW
    analyst_id                  VARCHAR(20),
    analyst_notes               TEXT,
    resolution_timestamp        TIMESTAMP_NTZ,
    resolution_hours            NUMBER(8,1),
    sanctions_list_matched      VARCHAR(50),
    is_pep_match                BOOLEAN         DEFAULT FALSE,
    is_adverse_media            BOOLEAN         DEFAULT FALSE,
    workflow_id                 VARCHAR(20),
    source_system               VARCHAR(50),
    created_at                  TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    updated_at                  TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),

    -- Metadata columns
    _loaded_at                  TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    _file_name                  VARCHAR(500),
    _file_row_number            NUMBER
)
DATA_RETENTION_TIME_IN_DAYS = 1
CLUSTER BY (screening_timestamp::DATE)
COMMENT = 'Raw sanctions screening results linked to trades'
;

ALTER TABLE RAW.RAW_SCREENING_RESULTS SET TAG
    GOVERNANCE.TAGS.DATA_DOMAIN = 'SCREENING',
    GOVERNANCE.TAGS.DATA_CLASSIFICATION = 'SANCTIONS_SENSITIVE';


-- ============================================================================
-- 7. STREAMING LANDING TABLES (for real-time ingestion via Snowpipe Streaming)
-- ============================================================================

CREATE OR REPLACE TABLE RAW.STREAMING_VESSEL_POSITIONS (
    vessel_id                   VARCHAR(20)     NOT NULL,
    timestamp                   TIMESTAMP_NTZ   NOT NULL,
    latitude                    NUMBER(10,6),
    longitude                   NUMBER(10,6),
    speed_knots                 NUMBER(5,1),
    heading                     NUMBER(5,1),
    source                      VARCHAR(20),
    received_at                 TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (timestamp::DATE)
COMMENT = 'Real-time vessel position streaming landing table'
;

CREATE OR REPLACE TABLE RAW.STREAMING_TRADE_EVENTS (
    event_id                    VARCHAR(50)     NOT NULL,
    trade_id                    VARCHAR(20),
    event_type                  VARCHAR(30),      -- CREATED, MODIFIED, CONFIRMED, SETTLED, CANCELLED
    event_timestamp             TIMESTAMP_NTZ   NOT NULL,
    event_data                  VARIANT,           -- Full event payload
    received_at                 TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Real-time trade event streaming landing table'
;

CREATE OR REPLACE TABLE RAW.STREAMING_SANCTIONS_ALERTS (
    alert_id                    VARCHAR(50)     NOT NULL,
    alert_type                  VARCHAR(30),
    severity                    VARCHAR(10),
    entity_id                   VARCHAR(50),
    entity_name                 VARCHAR(500),
    sanctions_list              VARCHAR(50),
    alert_timestamp             TIMESTAMP_NTZ   NOT NULL,
    alert_data                  VARIANT,
    received_at                 TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Real-time sanctions alert streaming landing table'
;


-- ============================================================================
-- 8. SEARCH OPTIMIZATION (for critical lookup tables)
-- ============================================================================

-- Enable search optimization on frequently queried columns
ALTER TABLE RAW.RAW_COUNTERPARTIES ADD SEARCH OPTIMIZATION
    ON EQUALITY(counterparty_id, legal_name, lei_code, swift_bic);

ALTER TABLE RAW.RAW_SANCTIONS_LISTS ADD SEARCH OPTIMIZATION
    ON EQUALITY(entity_id, entity_name, sanctions_list)
    ON SUBSTRING(entity_name);

ALTER TABLE RAW.RAW_VESSELS ADD SEARCH OPTIMIZATION
    ON EQUALITY(vessel_id, imo_number, vessel_name);

ALTER TABLE RAW.RAW_TRADES ADD SEARCH OPTIMIZATION
    ON EQUALITY(trade_id, buyer_counterparty_id, seller_counterparty_id, vessel_id);

ALTER TABLE RAW.RAW_SCREENING_RESULTS ADD SEARCH OPTIMIZATION
    ON EQUALITY(screening_id, trade_id, counterparty_id, sanctioned_entity_id);


-- ============================================================================
-- 9. VERIFICATION
-- ============================================================================

SHOW TRANSIENT TABLES IN SCHEMA RAW;
SHOW TABLES IN SCHEMA RAW;

SELECT
    TABLE_NAME,
    TABLE_TYPE,
    ROW_COUNT,
    BYTES,
    CLUSTERING_KEY,
    SEARCH_OPTIMIZATION,
    COMMENT
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'RAW'
ORDER BY TABLE_NAME;
