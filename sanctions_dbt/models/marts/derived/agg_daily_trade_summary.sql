{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='summary_date_key',
        tags=['marts', 'derived', 'daily'],
        cluster_by=['trade_date']
    )
}}

/*
    Derived Aggregate: Daily Trade Summary
    Pre-aggregated trade metrics per day, commodity group, desk, and risk tier.
    Powers executive dashboards and trend analysis.
*/

WITH trades AS (

    SELECT * FROM {{ ref('fct_trades') }}

    {% if is_incremental() %}
    WHERE trade_date > (SELECT COALESCE(MAX(trade_date), '1900-01-01') FROM {{ this }})
    {% endif %}

),

daily_summary AS (

    SELECT
        {{ surrogate_key(['trade_date', 'commodity_group', 'desk', 'screening_status']) }}
                                                        AS summary_date_key,
        trade_date,
        commodity_group,
        desk,
        screening_status,

        -- Volume metrics
        COUNT(*)                                        AS trade_count,
        COUNT(DISTINCT buyer_counterparty_id)           AS distinct_buyers,
        COUNT(DISTINCT seller_counterparty_id)          AS distinct_sellers,
        COUNT(DISTINCT vessel_id)                       AS distinct_vessels,

        -- Financial metrics
        SUM(total_value_usd)                            AS total_value_usd,
        AVG(total_value_usd)                            AS avg_trade_value_usd,
        MEDIAN(total_value_usd)                         AS median_trade_value_usd,
        SUM(quantity_mt)                                AS total_quantity_mt,

        -- Risk metrics
        AVG(sanctions_risk_score)                       AS avg_risk_score,
        MAX(sanctions_risk_score)                       AS max_risk_score,
        COUNT_IF(is_high_risk_trade)                    AS high_risk_trade_count,
        COUNT_IF(screening_status = 'ESCALATED')        AS escalated_count,
        COUNT_IF(screening_status = 'REVIEW_REQUIRED')  AS review_required_count,

        -- Counterparty risk
        COUNT_IF(buyer_is_sanctioned OR seller_is_sanctioned)
                                                        AS sanctioned_party_trades,
        COUNT_IF(buyer_is_pep OR seller_is_pep)         AS pep_involved_trades,
        COUNT_IF(vessel_flagged)                        AS flagged_vessel_trades,

        -- Trade type breakdown
        COUNT_IF(trade_type = 'PHYSICAL')               AS physical_trades,
        COUNT_IF(trade_type = 'PAPER')                  AS paper_trades,
        COUNT_IF(trade_type = 'SWAP')                   AS swap_trades,

        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ              AS _dbt_loaded_at

    FROM trades
    GROUP BY 1, 2, 3, 4, 5

)

SELECT * FROM daily_summary
