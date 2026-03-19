{{
    config(
        materialized='table',
        cluster_by=['trade_date', 'commodity_group']
    )
}}

/*
    Fact: Trade transactions enriched with counterparty risk context.
    912.5M+ records at production scale.
*/

WITH trade_counterparties AS (

    SELECT * FROM {{ ref('int_trade_counterparties') }}

),

final AS (

    SELECT
        {{ hash_surrogate_key(['tc.trade_id']) }}                   AS trade_sk,
        {{ hash_surrogate_key(['tc.buyer_counterparty_id']) }}      AS buyer_counterparty_sk,
        {{ hash_surrogate_key(['tc.seller_counterparty_id']) }}     AS seller_counterparty_sk,
        {{ hash_surrogate_key(['tc.vessel_id']) }}                  AS vessel_sk,

        tc.trade_id,
        tc.trade_reference,
        tc.trade_timestamp,
        tc.trade_date,
        tc.settlement_date,
        tc.trade_type,
        tc.trade_status,
        tc.commodity_code,
        tc.commodity_name,
        tc.commodity_group,
        tc.quantity_mt,
        tc.price_per_mt_usd,
        tc.total_value_usd,
        tc.currency,
        tc.fx_rate,
        tc.incoterm,
        tc.origin_country,
        tc.destination_country,
        tc.loading_port,
        tc.discharge_port,
        tc.vessel_id,
        tc.vessel_flagged,
        tc.sanctions_risk_score,
        tc.screening_status,
        tc.booking_entity,
        tc.trader_id,
        tc.desk,
        tc.source_system,

        -- Counterparty context
        tc.buyer_counterparty_id,
        tc.buyer_legal_name,
        tc.buyer_country,
        tc.buyer_risk_rating,
        tc.buyer_is_pep,
        tc.buyer_is_sanctioned,

        tc.seller_counterparty_id,
        tc.seller_legal_name,
        tc.seller_country,
        tc.seller_risk_rating,
        tc.seller_is_pep,
        tc.seller_is_sanctioned,

        tc.is_high_risk_trade,

        {{ audit_columns() }}

    FROM trade_counterparties tc

)

SELECT * FROM final
