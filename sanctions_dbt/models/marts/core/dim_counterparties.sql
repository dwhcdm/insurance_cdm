{{
    config(
        materialized='table',
        tags=['marts', 'core', 'daily'],
        cluster_by=['country_of_incorporation', 'risk_rating']
    )
}}

/*
    Dimension: Counterparties
    Fully enriched counterparty dimension with risk classification.
*/

WITH counterparties AS (

    SELECT * FROM {{ ref('stg_counterparties') }}

),

screening_summary AS (

    SELECT
        counterparty_id,
        COUNT(*)                                        AS total_screenings,
        COUNT_IF(disposition = 'TRUE_POSITIVE')         AS true_positive_count,
        COUNT_IF(disposition = 'FALSE_POSITIVE')        AS false_positive_count,
        COUNT_IF(disposition = 'ESCALATED')             AS escalated_count,
        COUNT_IF(disposition = 'SAR_FILED')             AS sar_filed_count,
        MAX(match_score)                                AS max_match_score,
        AVG(match_score)                                AS avg_match_score,
        MAX(screening_timestamp)                        AS last_screening_date

    FROM {{ ref('stg_screening_results') }}
    GROUP BY counterparty_id

),

final AS (

    SELECT
        {{ surrogate_key(['c.counterparty_id']) }}      AS counterparty_sk,
        c.counterparty_id,
        c.legal_name,
        c.entity_type,
        c.country_of_incorporation,
        c.country_of_domicile,
        c.registration_number,
        c.lei_code,
        c.swift_bic,
        c.tax_id,
        c.address_line_1,
        c.address_line_2,
        c.city,
        c.state_province,
        c.postal_code,
        c.industry_sector,
        c.risk_rating,
        c.is_pep,
        c.is_sanctioned,
        c.alias_names,
        c.registration_date,
        c.last_kyc_review_date,

        -- Screening metrics
        COALESCE(ss.total_screenings, 0)                AS total_screenings,
        COALESCE(ss.true_positive_count, 0)             AS true_positive_count,
        COALESCE(ss.false_positive_count, 0)            AS false_positive_count,
        COALESCE(ss.escalated_count, 0)                 AS escalated_count,
        COALESCE(ss.sar_filed_count, 0)                 AS sar_filed_count,
        ss.max_match_score,
        ss.avg_match_score,
        ss.last_screening_date,

        -- Composite risk tier
        CASE
            WHEN c.is_sanctioned THEN 'BLOCKED'
            WHEN ss.sar_filed_count > 0 THEN 'CRITICAL'
            WHEN c.risk_rating = 'HIGH' OR ss.true_positive_count > 0 THEN 'HIGH'
            WHEN c.risk_rating = 'MEDIUM' OR c.is_pep THEN 'ELEVATED'
            ELSE 'STANDARD'
        END                                             AS composite_risk_tier,

        c.source_system,
        c._dbt_loaded_at

    FROM counterparties c
    LEFT JOIN screening_summary ss
        ON c.counterparty_id = ss.counterparty_id

)

SELECT * FROM final
