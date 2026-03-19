{{
    config(
        materialized='ephemeral'
    )
}}

/*
    Intermediate: Enrich screening results with counterparty, sanctions list,
    and trade context for downstream analytics.
*/

WITH screening AS (

    SELECT * FROM {{ ref('stg_screening_results') }}

),

sanctions AS (

    SELECT * FROM {{ ref('stg_sanctions_lists') }}

),

counterparties AS (

    SELECT * FROM {{ ref('stg_counterparties') }}

),

enriched AS (

    SELECT
        s.screening_id,
        s.trade_id,
        s.counterparty_id,
        s.sanctioned_entity_id,
        s.screening_timestamp,
        s.screening_type,
        s.match_score,
        s.match_type,
        s.match_details,
        s.disposition,
        s.risk_level,
        s.analyst_id,
        s.analyst_notes,
        s.resolution_timestamp,
        s.resolution_hours,
        s.sanctions_list_matched,
        s.is_pep_match,
        s.is_adverse_media,
        s.workflow_id,
        s.source_system,

        -- Counterparty context
        cp.legal_name                                   AS counterparty_name,
        cp.entity_type                                  AS counterparty_entity_type,
        cp.country_of_incorporation                     AS counterparty_country,
        cp.risk_rating                                  AS counterparty_risk_rating,

        -- Sanctioned entity context
        sl.entity_name                                  AS sanctioned_entity_name,
        sl.entity_type                                  AS sanctioned_entity_type,
        sl.sanctions_list                               AS sanctioned_list_source,
        sl.sanctions_program,
        sl.is_active                                    AS sanctioned_entity_is_active,

        -- SLA tracking
        CASE
            WHEN s.risk_level = 'HIGH'   AND s.resolution_hours <= 4   THEN TRUE
            WHEN s.risk_level = 'MEDIUM' AND s.resolution_hours <= 24  THEN TRUE
            WHEN s.risk_level = 'LOW'    AND s.resolution_hours <= 72  THEN TRUE
            WHEN s.resolution_hours IS NULL THEN NULL
            ELSE FALSE
        END                                             AS is_within_sla,

        -- Classification
        CASE
            WHEN s.disposition = 'TRUE_POSITIVE' THEN TRUE
            ELSE FALSE
        END                                             AS is_true_positive,

        CASE
            WHEN s.disposition = 'FALSE_POSITIVE' THEN TRUE
            ELSE FALSE
        END                                             AS is_false_positive

    FROM screening s
    LEFT JOIN counterparties cp
        ON s.counterparty_id = cp.counterparty_id
    LEFT JOIN sanctions sl
        ON s.sanctioned_entity_id = sl.sanctioned_entity_id

)

SELECT * FROM enriched
