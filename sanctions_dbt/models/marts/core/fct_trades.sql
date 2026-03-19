{{
    config(
        materialized='table',
        tags=['marts', 'core', 'daily'],
        cluster_by=['trade_date', 'commodity_group']
    )
}}

/*
    Fact: Trades
    Core trade fact table enriched with counterparty risk context.
    912.5M+ records at production scale.
*/

WITH enriched_trades AS (

    SELECT * FROM {{ ref('int_trade_counterparties') }}

),

final AS (

    SELECT
        {{ surrogate_key(['trade_id']) }}                AS trade_sk,

        -- Keys
        trade_id,
        trade_reference,
        {{ surrogate_key(['buyer_counterparty_id']) }}   AS buyer_counterparty_sk,
        {{ surrogate_key(['seller_counterparty_id']) }}  AS seller_counterparty_sk,
        {{ surrogate_key(['vessel_id']) }}                AS vessel_sk,

        -- Natural keys
        buyer_counterparty_id,
        seller_counterparty_id,
        vessel_id,

        -- Temporal
        trade_timestamp,
        trade_date,
        settlement_date,

        -- Trade classification
        trade_type,
        trade_status,

        -- Counterparty context
        buyer_name,
        buyer_country,
        buyer_risk_rating,
        buyer_is_pep,
        buyer_is_sanctioned,
        seller_name,
        seller_country,
        seller_risk_rating,
        seller_is_pep,
        seller_is_sanctioned,

        -- Commodity
        commodity_code,
        commodity_name,
        commodity_group,

        -- Financial measures
        quantity_mt,
        price_per_mt_usd,
        total_value_usd,
        currency,
        fx_rate,

        -- Shipping
        incoterm,
        origin_country,
        destination_country,
        loading_port,
        discharge_port,

        -- Risk
        vessel_flagged,
        sanctions_risk_score,
        screening_status,
        is_high_risk_trade,

        -- Booking
        booking_entity,
        trader_id,
        desk,

        -- Metadata
        source_system,
        _dbt_loaded_at

    FROM enriched_trades

)

SELECT * FROM final
