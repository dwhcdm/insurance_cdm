WITH source AS (

    SELECT * FROM {{ source('raw', 'raw_trades') }}

),

renamed AS (

    SELECT
        trade_id,
        TRIM(trade_reference)                       AS trade_reference,
        trade_timestamp::TIMESTAMP_NTZ              AS trade_timestamp,
        trade_date::DATE                            AS trade_date,
        settlement_date::DATE                       AS settlement_date,
        UPPER(TRIM(trade_type))                     AS trade_type,
        UPPER(TRIM(trade_status))                   AS trade_status,
        buyer_counterparty_id,
        seller_counterparty_id,
        UPPER(TRIM(commodity_code))                 AS commodity_code,
        TRIM(commodity_name)                        AS commodity_name,
        UPPER(TRIM(commodity_group))                AS commodity_group,
        quantity_mt::NUMBER(18,4)                   AS quantity_mt,
        price_per_mt_usd::NUMBER(18,4)              AS price_per_mt_usd,
        total_value_usd::NUMBER(18,2)               AS total_value_usd,
        UPPER(TRIM(currency))                       AS currency,
        fx_rate::NUMBER(18,8)                       AS fx_rate,
        UPPER(TRIM(incoterm))                       AS incoterm,
        UPPER(TRIM(origin_country))                 AS origin_country,
        UPPER(TRIM(destination_country))            AS destination_country,
        TRIM(loading_port)                          AS loading_port,
        TRIM(discharge_port)                        AS discharge_port,
        vessel_id,
        vessel_flagged,
        sanctions_risk_score::NUMBER(5,2)           AS sanctions_risk_score,
        UPPER(TRIM(screening_status))               AS screening_status,
        TRIM(booking_entity)                        AS booking_entity,
        TRIM(trader_id)                             AS trader_id,
        UPPER(TRIM(desk))                           AS desk,
        TRIM(source_system)                         AS source_system,
        _loaded_at

    FROM source

)

SELECT * FROM renamed
