{{
    config(
        materialized='view',
        tags=['staging', 'daily']
    )
}}

/*
    Staging model: Trades
    Source: RAW.RAW_TRADES
    Cleanse and standardize commodity trade transactions.
    THIS IS THE BIG TABLE - 912.5M+ records at production scale.
*/

WITH source AS (

    SELECT * FROM {{ source('raw', 'raw_trades') }}

),

renamed AS (

    SELECT
        -- Primary Key
        trade_id,
        TRIM(trade_reference)                           AS trade_reference,

        -- Temporal
        trade_timestamp::TIMESTAMP_NTZ                  AS trade_timestamp,
        trade_date::DATE                                AS trade_date,
        settlement_date::DATE                           AS settlement_date,

        -- Trade classification
        UPPER(TRIM(trade_type))                         AS trade_type,
        UPPER(TRIM(trade_status))                       AS trade_status,

        -- Counterparties (FKs)
        buyer_counterparty_id,
        seller_counterparty_id,

        -- Commodity
        UPPER(TRIM(commodity_code))                     AS commodity_code,
        UPPER(TRIM(commodity_name))                     AS commodity_name,
        UPPER(TRIM(commodity_group))                    AS commodity_group,

        -- Financial measures
        COALESCE(quantity_mt, 0)::NUMBER(18,4)          AS quantity_mt,
        COALESCE(price_per_mt_usd, 0)::NUMBER(18,4)    AS price_per_mt_usd,
        COALESCE(total_value_usd, 0)::NUMBER(18,2)     AS total_value_usd,
        UPPER(TRIM(currency))                           AS currency,
        COALESCE(fx_rate, 1.0)::NUMBER(18,8)            AS fx_rate,

        -- Shipping terms
        UPPER(TRIM(incoterm))                           AS incoterm,

        -- Geography
        UPPER(TRIM(origin_country))                     AS origin_country,
        UPPER(TRIM(destination_country))                AS destination_country,
        UPPER(TRIM(loading_port))                       AS loading_port,
        UPPER(TRIM(discharge_port))                     AS discharge_port,

        -- Vessel (physical trades only)
        vessel_id,
        COALESCE(vessel_flagged, FALSE)                 AS vessel_flagged,

        -- Risk scoring
        COALESCE(sanctions_risk_score, 0)::NUMBER(5,2)  AS sanctions_risk_score,
        UPPER(TRIM(screening_status))                   AS screening_status,

        -- Booking
        UPPER(TRIM(booking_entity))                     AS booking_entity,
        TRIM(trader_id)                                 AS trader_id,
        UPPER(TRIM(desk))                               AS desk,

        -- Metadata
        source_system,
        _loaded_at,
        {{ audit_columns() }}

    FROM source

)

SELECT * FROM renamed
