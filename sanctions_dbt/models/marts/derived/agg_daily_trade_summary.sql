{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['trade_date', 'commodity_group', 'desk', 'screening_status'],
        cluster_by=['trade_date']
    )
}}

/*
    Aggregate: Daily trade metrics by commodity group, desk, and screening status.
*/

WITH trades AS (

    SELECT * FROM {{ ref('fct_trades') }}
    {% if is_incremental() %}
    WHERE trade_date > (SELECT MAX(trade_date) - 1 FROM {{ this }})
    {% endif %}

),

aggregated AS (

    SELECT
        trade_date,
        commodity_group,
        desk,
        screening_status,

        COUNT(*)                                    AS trade_count,
        COUNT(DISTINCT buyer_counterparty_id)       AS distinct_buyers,
        COUNT(DISTINCT seller_counterparty_id)      AS distinct_sellers,
        COUNT(DISTINCT vessel_id)                   AS distinct_vessels,

        SUM(total_value_usd)                        AS total_value_usd,
        AVG(total_value_usd)                        AS avg_trade_value_usd,
        MEDIAN(total_value_usd)                     AS median_trade_value_usd,
        SUM(quantity_mt)                            AS total_quantity_mt,

        AVG(sanctions_risk_score)                   AS avg_risk_score,
        MAX(sanctions_risk_score)                   AS max_risk_score,

        COUNT_IF(is_high_risk_trade)                AS high_risk_trade_count,
        COUNT_IF(screening_status = 'ESCALATED')    AS escalated_count,
        COUNT_IF(screening_status = 'REVIEW_REQUIRED') AS review_required_count,
        COUNT_IF(buyer_is_sanctioned OR seller_is_sanctioned) AS sanctioned_party_trades,
        COUNT_IF(buyer_is_pep OR seller_is_pep)     AS pep_involved_trades,
        COUNT_IF(vessel_flagged)                    AS flagged_vessel_trades,

        COUNT_IF(trade_type = 'PHYSICAL')           AS physical_trades,
        COUNT_IF(trade_type = 'PAPER')              AS paper_trades,
        COUNT_IF(trade_type = 'SWAP')               AS swap_trades,

        {{ audit_columns() }}

    FROM trades
    GROUP BY trade_date, commodity_group, desk, screening_status

)

SELECT * FROM aggregated
