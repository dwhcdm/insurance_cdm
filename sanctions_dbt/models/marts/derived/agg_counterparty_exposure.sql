{{
    config(
        materialized='table',
        cluster_by=['composite_risk_tier']
    )
}}

/*
    Aggregate: Counterparty trade exposure (buy-side and sell-side).
*/

WITH counterparties AS (

    SELECT * FROM {{ ref('dim_counterparties') }}

),

trades AS (

    SELECT * FROM {{ ref('fct_trades') }}

),

buy_exposure AS (

    SELECT
        buyer_counterparty_id                       AS counterparty_id,
        COUNT(*)                                    AS buy_trade_count,
        SUM(total_value_usd)                        AS buy_total_value_usd,
        AVG(sanctions_risk_score)                   AS buy_avg_risk_score,
        COUNT_IF(is_high_risk_trade)                AS buy_high_risk_count

    FROM trades
    GROUP BY buyer_counterparty_id

),

sell_exposure AS (

    SELECT
        seller_counterparty_id                      AS counterparty_id,
        COUNT(*)                                    AS sell_trade_count,
        SUM(total_value_usd)                        AS sell_total_value_usd,
        AVG(sanctions_risk_score)                   AS sell_avg_risk_score,
        COUNT_IF(is_high_risk_trade)                AS sell_high_risk_count

    FROM trades
    GROUP BY seller_counterparty_id

),

final AS (

    SELECT
        c.counterparty_sk,
        c.counterparty_id,
        c.legal_name,
        c.entity_type,
        c.country_of_incorporation,
        c.risk_rating,
        c.is_pep,
        c.is_sanctioned,
        c.composite_risk_tier,

        -- Buy-side exposure
        COALESCE(be.buy_trade_count, 0)             AS buy_trade_count,
        COALESCE(be.buy_total_value_usd, 0)         AS buy_total_value_usd,
        be.buy_avg_risk_score,
        COALESCE(be.buy_high_risk_count, 0)         AS buy_high_risk_count,

        -- Sell-side exposure
        COALESCE(se.sell_trade_count, 0)             AS sell_trade_count,
        COALESCE(se.sell_total_value_usd, 0)         AS sell_total_value_usd,
        se.sell_avg_risk_score,
        COALESCE(se.sell_high_risk_count, 0)         AS sell_high_risk_count,

        -- Combined exposure
        COALESCE(be.buy_trade_count, 0)
            + COALESCE(se.sell_trade_count, 0)      AS total_trade_count,
        COALESCE(be.buy_total_value_usd, 0)
            + COALESCE(se.sell_total_value_usd, 0)  AS total_exposure_usd,
        COALESCE(be.buy_high_risk_count, 0)
            + COALESCE(se.sell_high_risk_count, 0)  AS total_high_risk_count,

        -- Screening summary
        c.total_screenings,
        c.true_positive_count,
        c.false_positive_count,
        c.sar_filed_count,

        {{ audit_columns() }}

    FROM counterparties c
    LEFT JOIN buy_exposure be ON c.counterparty_id = be.counterparty_id
    LEFT JOIN sell_exposure se ON c.counterparty_id = se.counterparty_id

)

SELECT * FROM final
