{{
    config(
        materialized='table',
        tags=['marts', 'derived', 'daily'],
        cluster_by=['composite_risk_tier']
    )
}}

/*
    Derived Aggregate: Counterparty Exposure
    Aggregates total trade exposure per counterparty for risk management.
    Includes both buy-side and sell-side exposure.
*/

WITH trades AS (

    SELECT * FROM {{ ref('fct_trades') }}
    WHERE trade_status != 'CANCELLED'

),

counterparties AS (

    SELECT * FROM {{ ref('dim_counterparties') }}

),

buy_exposure AS (

    SELECT
        buyer_counterparty_id                           AS counterparty_id,
        COUNT(*)                                        AS buy_trade_count,
        SUM(total_value_usd)                            AS buy_total_value_usd,
        AVG(sanctions_risk_score)                       AS buy_avg_risk_score,
        COUNT_IF(is_high_risk_trade)                    AS buy_high_risk_count,
        MIN(trade_date)                                 AS first_buy_date,
        MAX(trade_date)                                 AS last_buy_date

    FROM trades
    GROUP BY 1

),

sell_exposure AS (

    SELECT
        seller_counterparty_id                          AS counterparty_id,
        COUNT(*)                                        AS sell_trade_count,
        SUM(total_value_usd)                            AS sell_total_value_usd,
        AVG(sanctions_risk_score)                       AS sell_avg_risk_score,
        COUNT_IF(is_high_risk_trade)                    AS sell_high_risk_count,
        MIN(trade_date)                                 AS first_sell_date,
        MAX(trade_date)                                 AS last_sell_date

    FROM trades
    GROUP BY 1

),

final AS (

    SELECT
        c.counterparty_sk,
        c.counterparty_id,
        c.legal_name,
        c.entity_type,
        c.country_of_incorporation,
        c.risk_rating,
        c.composite_risk_tier,
        c.is_pep,
        c.is_sanctioned,

        -- Buy-side exposure
        COALESCE(b.buy_trade_count, 0)                  AS buy_trade_count,
        COALESCE(b.buy_total_value_usd, 0)              AS buy_total_value_usd,
        b.buy_avg_risk_score,
        COALESCE(b.buy_high_risk_count, 0)              AS buy_high_risk_count,
        b.first_buy_date,
        b.last_buy_date,

        -- Sell-side exposure
        COALESCE(s.sell_trade_count, 0)                  AS sell_trade_count,
        COALESCE(s.sell_total_value_usd, 0)              AS sell_total_value_usd,
        s.sell_avg_risk_score,
        COALESCE(s.sell_high_risk_count, 0)              AS sell_high_risk_count,
        s.first_sell_date,
        s.last_sell_date,

        -- Combined exposure
        COALESCE(b.buy_trade_count, 0)
            + COALESCE(s.sell_trade_count, 0)            AS total_trade_count,
        COALESCE(b.buy_total_value_usd, 0)
            + COALESCE(s.sell_total_value_usd, 0)        AS total_exposure_usd,
        COALESCE(b.buy_high_risk_count, 0)
            + COALESCE(s.sell_high_risk_count, 0)        AS total_high_risk_count,

        -- Screening summary
        c.total_screenings,
        c.true_positive_count,
        c.false_positive_count,
        c.sar_filed_count,

        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ              AS _dbt_loaded_at

    FROM counterparties c
    LEFT JOIN buy_exposure b  ON c.counterparty_id = b.counterparty_id
    LEFT JOIN sell_exposure s ON c.counterparty_id = s.counterparty_id

)

SELECT * FROM final
