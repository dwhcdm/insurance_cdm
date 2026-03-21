{{
    config(
        materialized='ephemeral'
    )
}}

/*
    Enriches trades with buyer & seller counterparty details.
    Resolves both counterparty FKs and attaches risk attributes.
    Derives is_high_risk_trade flag.
*/

WITH trades AS (

    SELECT * FROM {{ ref('stg_trades') }}

),

counterparties AS (

    SELECT * FROM {{ ref('stg_counterparties') }}

),

enriched AS (

    SELECT
        t.trade_id,
        t.trade_reference,
        t.trade_timestamp,
        t.trade_date,
        t.settlement_date,
        t.trade_type,
        t.trade_status,
        t.commodity_code,
        t.commodity_name,
        t.commodity_group,
        t.quantity_mt,
        t.price_per_mt_usd,
        t.total_value_usd,
        t.currency,
        t.fx_rate,
        t.incoterm,
        t.origin_country,
        t.destination_country,
        t.loading_port,
        t.discharge_port,
        t.vessel_id,
        t.vessel_flagged,
        t.sanctions_risk_score,
        t.screening_status,
        t.booking_entity,
        t.trader_id,
        t.desk,
        t.source_system,

        -- Buyer counterparty
        t.buyer_counterparty_id,
        bc.legal_name                               AS buyer_legal_name,
        bc.entity_type                              AS buyer_entity_type,
        bc.country_of_incorporation                 AS buyer_country,
        bc.risk_rating                              AS buyer_risk_rating,
        bc.is_pep                                   AS buyer_is_pep,
        bc.is_sanctioned                            AS buyer_is_sanctioned,

        -- Seller counterparty
        t.seller_counterparty_id,
        sc.legal_name                               AS seller_legal_name,
        sc.entity_type                              AS seller_entity_type,
        sc.country_of_incorporation                 AS seller_country,
        sc.risk_rating                              AS seller_risk_rating,
        sc.is_pep                                   AS seller_is_pep,
        sc.is_sanctioned                            AS seller_is_sanctioned,

        -- Derived: High risk trade flag
        CASE
            WHEN COALESCE(bc.is_sanctioned, FALSE) OR COALESCE(sc.is_sanctioned, FALSE) THEN TRUE
            WHEN COALESCE(bc.is_pep, FALSE) OR COALESCE(sc.is_pep, FALSE) THEN TRUE
            WHEN COALESCE(t.vessel_flagged, FALSE) THEN TRUE
            WHEN t.sanctions_risk_score >= 70 THEN TRUE
            ELSE FALSE
        END                                         AS is_high_risk_trade

    FROM trades t
    LEFT JOIN counterparties bc ON t.buyer_counterparty_id = bc.counterparty_id
    LEFT JOIN counterparties sc ON t.seller_counterparty_id = sc.counterparty_id

)

SELECT * FROM enriched
