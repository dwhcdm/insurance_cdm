-- ============================================================================
-- 003_raw_tables.sql
-- Raw table definitions for Bronze layer with governance tags
-- ============================================================================
-- EXECUTION ORDER: Run AFTER 002_external_stages.sql
-- ============================================================================

USE ROLE SANCTIONS_DATA_ENGINEER;
USE DATABASE SANCTIONS_DEV;
USE SCHEMA RAW;

-- ============================================================================
-- SECTION 1: DIMENSION / REFERENCE TABLES
-- ============================================================================

-- Counterparty / KYC master data (5M records target)
CREATE OR REPLACE TRANSIENT TABLE RAW_COUNTERPARTIES (
    counterparty_id         VARCHAR(20)     NOT NULL,
    legal_name              VARCHAR(500)    NOT NULL,
    entity_type             VARCHAR(20)     NOT NULL,      -- CORPORATE, INDIVIDUAL, GOVERNMENT, BANK, TRADER
    country_of_incorporation VARCHAR(3)     NOT NULL,      -- ISO 3166-1 alpha-2
    country_of_domicile     VARCHAR(3),
    registration_number     VARCHAR(100),
    lei_code                VARCHAR(20),                   -- Legal Entity Identifier
    swift_bic               VARCHAR(11),
    tax_id                  VARCHAR(50),
    address_line_1          VARCHAR(500),
    address_line_2          VARCHAR(500),
    city                    VARCHAR(200),
    state_province          VARCHAR(200),
    postal_code             VARCHAR(20),
    industry_sector         VARCHAR(100),
    risk_rating             VARCHAR(10)     NOT NULL,      -- HIGH, MEDIUM, LOW
    is_pep                  BOOLEAN         DEFAULT FALSE,
    is_sanctioned           BOOLEAN         DEFAULT FALSE,
    alias_names             VARIANT,                       -- JSON array of aliases
    registration_date       DATE,
    last_kyc_review_date    DATE,
    source_system           VARCHAR(50)     NOT NULL,
    _loaded_at              TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
)
DATA_RETENTION_TIME_IN_DAYS = 1
COMMENT = 'Raw counterparty/entity data from KYC systems'
;

-- Apply governance tags
ALTER TABLE RAW_COUNTERPARTIES SET TAG
    SANCTIONS_PROD.GOVERNANCE.DATA_DOMAIN = 'COUNTERPARTY',
    SANCTIONS_PROD.GOVERNANCE.DATA_LAYER = 'RAW',
    SANCTIONS_PROD.GOVERNANCE.DATA_CLASSIFICATION = 'CONFIDENTIAL',
    SANCTIONS_PROD.GOVERNANCE.PII_FLAG = 'TRUE';

-- Sanctions list entries (250K records target)
CREATE OR REPLACE TRANSIENT TABLE RAW_SANCTIONS_LISTS (
    entity_id               VARCHAR(20)     NOT NULL,
    entity_name             VARCHAR(500)    NOT NULL,
    entity_type             VARCHAR(20)     NOT NULL,      -- INDIVIDUAL, ENTITY, VESSEL, AIRCRAFT
    sanctions_list          VARCHAR(50)     NOT NULL,      -- OFAC_SDN, EU_SANCTIONS, UN_SANCTIONS, etc.
    sanctions_program       VARCHAR(100),
    nationality             VARCHAR(3),
    country_codes           VARIANT,                       -- JSON array of associated countries
    alias_names             VARIANT,                       -- JSON array of known aliases
    identification_numbers  VARIANT,                       -- JSON array of ID numbers
    listed_date             DATE            NOT NULL,
    delisted_date           DATE,
    last_updated            TIMESTAMP_NTZ,
    remarks                 VARCHAR(4000),
    source_url              VARCHAR(1000),
    source_system           VARCHAR(50)     NOT NULL,
    _loaded_at              TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    _file_name              VARCHAR(500)
)
DATA_RETENTION_TIME_IN_DAYS = 1
COMMENT = 'Raw sanctions list data from OFAC/EU/UN/UK regulatory sources'
;

ALTER TABLE RAW_SANCTIONS_LISTS SET TAG
    SANCTIONS_PROD.GOVERNANCE.DATA_DOMAIN = 'SANCTIONS',
    SANCTIONS_PROD.GOVERNANCE.DATA_LAYER = 'RAW',
    SANCTIONS_PROD.GOVERNANCE.DATA_CLASSIFICATION = 'RESTRICTED',
    SANCTIONS_PROD.GOVERNANCE.PII_FLAG = 'TRUE';

-- Vessel master data (500K records target)
CREATE OR REPLACE TRANSIENT TABLE RAW_VESSELS (
    vessel_id               VARCHAR(20)     NOT NULL,
    imo_number              VARCHAR(10)     NOT NULL,      -- IMO vessel identification
    mmsi                    VARCHAR(9),                    -- Maritime Mobile Service Identity
    vessel_name             VARCHAR(200)    NOT NULL,
    call_sign               VARCHAR(10),
    vessel_type             VARCHAR(50)     NOT NULL,      -- VLCC, SUEZMAX, AFRAMAX, etc.
    flag_state              VARCHAR(3)      NOT NULL,      -- ISO country code
    class_society           VARCHAR(100),
    dwt                     NUMBER(12,2),                  -- Deadweight tonnage
    gross_tonnage           NUMBER(12,2),
    year_built              INTEGER,
    builder                 VARCHAR(200),
    status                  VARCHAR(20)     DEFAULT 'ACTIVE',
    is_flagged              BOOLEAN         DEFAULT FALSE,
    registered_owner        VARCHAR(500),
    beneficial_owner        VARCHAR(500),
    source_system           VARCHAR(50)     NOT NULL,
    _loaded_at              TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
)
DATA_RETENTION_TIME_IN_DAYS = 1
COMMENT = 'Raw vessel master data including ownership and flag state'
;

ALTER TABLE RAW_VESSELS SET TAG
    SANCTIONS_PROD.GOVERNANCE.DATA_DOMAIN = 'VESSEL',
    SANCTIONS_PROD.GOVERNANCE.DATA_LAYER = 'RAW',
    SANCTIONS_PROD.GOVERNANCE.DATA_CLASSIFICATION = 'INTERNAL';

-- ============================================================================
-- SECTION 2: FACT / TRANSACTIONAL TABLES
-- ============================================================================

-- Trade transactions - THE BIG TABLE (2.5M/day = 912.5M/year)
CREATE OR REPLACE TRANSIENT TABLE RAW_TRADES (
    trade_id                VARCHAR(20)     NOT NULL,
    trade_reference         VARCHAR(50),
    trade_timestamp         TIMESTAMP_NTZ   NOT NULL,
    trade_date              DATE            NOT NULL,
    settlement_date         DATE,
    trade_type              VARCHAR(20)     NOT NULL,      -- PHYSICAL, PAPER, SWAP, OPTION, FUTURE
    trade_status            VARCHAR(20)     NOT NULL,      -- CONFIRMED, PENDING, SETTLED, CANCELLED
    buyer_counterparty_id   VARCHAR(20)     NOT NULL,
    seller_counterparty_id  VARCHAR(20)     NOT NULL,
    commodity_code          VARCHAR(20)     NOT NULL,
    commodity_name          VARCHAR(200),
    commodity_group         VARCHAR(50),
    quantity_mt             NUMBER(18,4),
    price_per_mt_usd        NUMBER(18,4),
    total_value_usd         NUMBER(18,2)    NOT NULL,
    currency                VARCHAR(3)      DEFAULT 'USD',
    fx_rate                 NUMBER(18,8)    DEFAULT 1.0,
    incoterm                VARCHAR(10),
    origin_country          VARCHAR(3),
    destination_country     VARCHAR(3),
    loading_port            VARCHAR(100),
    discharge_port          VARCHAR(100),
    vessel_id               VARCHAR(20),
    vessel_flagged          BOOLEAN         DEFAULT FALSE,
    sanctions_risk_score    NUMBER(5,2)     DEFAULT 0,
    screening_status        VARCHAR(20),
    booking_entity          VARCHAR(100),
    trader_id               VARCHAR(20),
    desk                    VARCHAR(50),
    source_system           VARCHAR(50)     NOT NULL,
    _loaded_at              TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    _file_name              VARCHAR(500)
)
DATA_RETENTION_TIME_IN_DAYS = 1
CLUSTER BY (trade_date)
COMMENT = 'Raw trade transactions - 2.5M records per day at production scale'
;

-- Enable search optimization for critical lookup columns
ALTER TABLE RAW_TRADES ADD SEARCH OPTIMIZATION ON EQUALITY(
    trade_id, trade_reference, buyer_counterparty_id,
    seller_counterparty_id, vessel_id, commodity_code
);

ALTER TABLE RAW_TRADES SET TAG
    SANCTIONS_PROD.GOVERNANCE.DATA_DOMAIN = 'TRADE',
    SANCTIONS_PROD.GOVERNANCE.DATA_LAYER = 'RAW',
    SANCTIONS_PROD.GOVERNANCE.DATA_CLASSIFICATION = 'CONFIDENTIAL';

-- Vessel movements / AIS data - THE BILLIONS TABLE (10M/day = 3.65B/year)
CREATE OR REPLACE TRANSIENT TABLE RAW_VESSEL_MOVEMENTS (
    movement_id             VARCHAR(20)     NOT NULL,
    vessel_id               VARCHAR(20)     NOT NULL,
    timestamp               TIMESTAMP_NTZ   NOT NULL,
    latitude                NUMBER(10,6)    NOT NULL,
    longitude               NUMBER(11,6)    NOT NULL,
    speed_knots             NUMBER(6,1),
    heading                 NUMBER(5,1),
    port_of_call            VARCHAR(100),
    origin_port             VARCHAR(100),
    destination_port        VARCHAR(100),
    voyage_id               VARCHAR(100),
    is_dark_activity        BOOLEAN         DEFAULT FALSE,
    dark_duration_hours     NUMBER(6,1),
    zone_risk_score         NUMBER(5,2)     DEFAULT 0,
    is_near_sanctioned_zone BOOLEAN         DEFAULT FALSE,
    ais_message_type        INTEGER,
    navigation_status       VARCHAR(50),
    source_system           VARCHAR(50)     NOT NULL,
    _loaded_at              TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
)
DATA_RETENTION_TIME_IN_DAYS = 1
CLUSTER BY (timestamp, vessel_id)
COMMENT = 'Raw AIS vessel movement data - 10M records per day (BILLIONS scale)'
;

ALTER TABLE RAW_VESSEL_MOVEMENTS ADD SEARCH OPTIMIZATION ON EQUALITY(
    vessel_id, voyage_id
);

ALTER TABLE RAW_VESSEL_MOVEMENTS SET TAG
    SANCTIONS_PROD.GOVERNANCE.DATA_DOMAIN = 'VESSEL',
    SANCTIONS_PROD.GOVERNANCE.DATA_LAYER = 'RAW',
    SANCTIONS_PROD.GOVERNANCE.DATA_CLASSIFICATION = 'INTERNAL';

-- Screening results (375K/day = 136.9M/year)
CREATE OR REPLACE TRANSIENT TABLE RAW_SCREENING_RESULTS (
    screening_id            VARCHAR(20)     NOT NULL,
    trade_id                VARCHAR(20)     NOT NULL,
    counterparty_id         VARCHAR(20),
    sanctioned_entity_id    VARCHAR(20),
    screening_timestamp     TIMESTAMP_NTZ   NOT NULL,
    screening_type          VARCHAR(20)     NOT NULL,      -- PRE_TRADE, POST_TRADE, PERIODIC, EVENT_DRIVEN
    match_score             NUMBER(6,4),                   -- 0.0000 to 1.0000
    match_type              VARCHAR(30),
    match_details           VARIANT,                       -- JSON match detail payload
    disposition             VARCHAR(30),                   -- TRUE_POSITIVE, FALSE_POSITIVE, ESCALATED, etc.
    risk_level              VARCHAR(10),
    analyst_id              VARCHAR(20),
    analyst_notes           VARCHAR(4000),
    resolution_timestamp    TIMESTAMP_NTZ,
    resolution_hours        NUMBER(8,2),
    sanctions_list_matched  VARCHAR(50),
    is_pep_match            BOOLEAN         DEFAULT FALSE,
    is_adverse_media        BOOLEAN         DEFAULT FALSE,
    workflow_id             VARCHAR(20),
    source_system           VARCHAR(50)     NOT NULL,
    _loaded_at              TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
)
DATA_RETENTION_TIME_IN_DAYS = 1
CLUSTER BY (screening_timestamp)
COMMENT = 'Raw sanctions screening results - 375K records per day'
;

ALTER TABLE RAW_SCREENING_RESULTS ADD SEARCH OPTIMIZATION ON EQUALITY(
    screening_id, trade_id, counterparty_id, sanctioned_entity_id
);

ALTER TABLE RAW_SCREENING_RESULTS SET TAG
    SANCTIONS_PROD.GOVERNANCE.DATA_DOMAIN = 'SCREENING',
    SANCTIONS_PROD.GOVERNANCE.DATA_LAYER = 'RAW',
    SANCTIONS_PROD.GOVERNANCE.DATA_CLASSIFICATION = 'RESTRICTED';

-- ============================================================================
-- SECTION 3: STREAMING LANDING TABLES
-- ============================================================================

-- Real-time vessel position landing table (Snowpipe Streaming target)
CREATE OR REPLACE TRANSIENT TABLE STREAMING_VESSEL_POSITIONS (
    vessel_id               VARCHAR(20)     NOT NULL,
    timestamp               TIMESTAMP_NTZ   NOT NULL,
    latitude                NUMBER(10,6)    NOT NULL,
    longitude               NUMBER(11,6)    NOT NULL,
    speed_knots             NUMBER(6,1),
    heading                 NUMBER(5,1),
    source_system           VARCHAR(50)     DEFAULT 'AIS_STREAM',
    _loaded_at              TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
)
DATA_RETENTION_TIME_IN_DAYS = 1
COMMENT = 'Streaming landing table for real-time AIS vessel positions'
;

-- Real-time trade event landing table
CREATE OR REPLACE TRANSIENT TABLE STREAMING_TRADE_EVENTS (
    trade_id                VARCHAR(20)     NOT NULL,
    event_type              VARCHAR(30)     NOT NULL,      -- NEW, AMENDED, CANCELLED, SETTLED
    event_timestamp         TIMESTAMP_NTZ   NOT NULL,
    trade_payload           VARIANT         NOT NULL,      -- Full trade JSON
    source_system           VARCHAR(50)     DEFAULT 'ETRM_STREAM',
    _loaded_at              TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
)
DATA_RETENTION_TIME_IN_DAYS = 1
COMMENT = 'Streaming landing table for real-time trade events'
;

-- Real-time sanctions alert landing table
CREATE OR REPLACE TRANSIENT TABLE STREAMING_SANCTIONS_ALERTS (
    alert_id                VARCHAR(20)     NOT NULL,
    alert_type              VARCHAR(30)     NOT NULL,
    severity                VARCHAR(10)     NOT NULL,      -- CRITICAL, HIGH, MEDIUM, LOW
    alert_timestamp         TIMESTAMP_NTZ   NOT NULL,
    alert_payload           VARIANT         NOT NULL,      -- Full alert JSON
    source_system           VARCHAR(50)     DEFAULT 'SCREENING_ENGINE',
    _loaded_at              TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
)
DATA_RETENTION_TIME_IN_DAYS = 1
COMMENT = 'Streaming landing table for real-time sanctions alerts'
;

-- End of 003_raw_tables.sql
