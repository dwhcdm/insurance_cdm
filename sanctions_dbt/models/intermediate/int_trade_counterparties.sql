{{
    config(
        materialized='ephemeral'
    )
}}

/*
    Intermediate: Enrich trades with buyer & seller counterparty details.
    Resolves both counterparty FKs and attaches risk attributes.
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

        -- Buyer
        t.buyer_counterparty_id,
        buyer.legal_name                                AS buyer_name,
        buyer.entity_type                               AS buyer_entity_type,
        buyer.country_of_incorporation                  AS buyer_country,
        buyer.risk_rating                               AS buyer_risk_rating,
        buyer.is_pep                                    AS buyer_is_pep,
        buyer.is_sanctioned                             AS buyer_is_sanctioned,

        -- Seller
        t.seller_counterparty_id,
        seller.legal_name                               AS seller_name,
        seller.entity_type                              AS seller_entity_type,
        seller.country_of_incorporation                 AS seller_country,
        seller.risk_rating                              AS seller_risk_rating,
        seller.is_pep                                   AS seller_is_pep,
        seller.is_sanctioned                            AS seller_is_sanctioned,

        -- Commodity
        t.commodity_code,
        t.commodity_name,
        t.commodity_group,

        -- Financial
        t.quantity_mt,
        t.price_per_mt_usd,
        t.total_value_usd,
        t.currency,
        t.fx_rate,

        -- Shipping
        t.incoterm,
        t.origin_country,
        t.destination_country,
        t.loading_port,
        t.discharge_port,

        -- Vessel
        t.vessel_id,
        t.vessel_flagged,

        -- Risk
        t.sanctions_risk_score,
        t.screening_status,

        -- Derived: composite risk flag
        CASE
            WHEN buyer.is_sanctioned OR seller.is_sanctioned THEN TRUE
            WHEN buyer.is_pep OR seller.is_pep THEN TRUE
            WHEN t.vessel_flagged THEN TRUE
            WHEN t.sanctions_risk_score >= 70 THEN TRUE
            ELSE FALSE
        END                                             AS is_high_risk_trade,

        -- Booking
        t.booking_entity,
        t.trader_id,
        t.desk,
        t.source_system,
        t._dbt_loaded_at

    FROM trades t
    LEFT JOIN counterparties buyer
        ON t.buyer_counterparty_id = buyer.counterparty_id
    LEFT JOIN counterparties seller
        ON t.seller_counterparty_id = seller.counterparty_id

)

SELECT * FROM enriched
