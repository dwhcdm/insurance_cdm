{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['screening_date', 'screening_type', 'risk_level', 'source_system'],
        cluster_by=['screening_date']
    )
}}

/*
    Aggregate: Screening team performance metrics with SLA tracking.
*/

WITH screening AS (

    SELECT * FROM {{ ref('fct_screening_results') }}
    {% if is_incremental() %}
    WHERE screening_timestamp::DATE > (SELECT MAX(screening_date) - 1 FROM {{ this }})
    {% endif %}

),

aggregated AS (

    SELECT
        screening_timestamp::DATE                   AS screening_date,
        screening_type,
        risk_level,
        source_system,

        COUNT(*)                                    AS total_screenings,
        COUNT(DISTINCT trade_id)                    AS distinct_trades_screened,
        COUNT(DISTINCT counterparty_id)             AS distinct_counterparties_screened,

        COUNT_IF(is_true_positive)                  AS true_positives,
        COUNT_IF(is_false_positive)                 AS false_positives,
        COUNT_IF(disposition = 'ESCALATED')         AS escalations,
        COUNT_IF(disposition = 'PENDING_REVIEW')    AS pending_reviews,
        COUNT_IF(disposition = 'SAR_FILED')         AS sars_filed,

        ROUND(COUNT_IF(is_true_positive) * 100.0 / NULLIF(COUNT(*), 0), 2) AS true_positive_rate_pct,
        ROUND(COUNT_IF(is_false_positive) * 100.0 / NULLIF(COUNT(*), 0), 2) AS false_positive_rate_pct,

        COUNT_IF(is_within_sla)                     AS within_sla_count,
        COUNT_IF(is_within_sla = FALSE)             AS sla_breach_count,
        ROUND(
            COUNT_IF(is_within_sla) * 100.0
            / NULLIF(COUNT_IF(is_within_sla) + COUNT_IF(is_within_sla = FALSE), 0),
        2)                                          AS sla_compliance_rate_pct,

        AVG(resolution_hours)                       AS avg_resolution_hours,
        MEDIAN(resolution_hours)                    AS median_resolution_hours,
        MAX(resolution_hours)                       AS max_resolution_hours,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY resolution_hours) AS p95_resolution_hours,

        AVG(match_score)                            AS avg_match_score,
        MAX(match_score)                            AS max_match_score,
        COUNT(DISTINCT analyst_id)                  AS distinct_analysts,

        {{ audit_columns() }}

    FROM screening
    GROUP BY screening_timestamp::DATE, screening_type, risk_level, source_system

)

SELECT * FROM aggregated
