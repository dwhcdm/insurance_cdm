{{
    config(
        materialized='ephemeral'
    )
}}

/*
    Enriches screening results with counterparty and sanctions list context.
    Derives SLA compliance, true/false positive flags.
*/

WITH screening AS (

    SELECT * FROM {{ ref('stg_screening_results') }}

),

counterparties AS (

    SELECT * FROM {{ ref('stg_counterparties') }}

),

sanctions_lists AS (

    SELECT * FROM {{ ref('stg_sanctions_lists') }}

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
        c.legal_name                                AS counterparty_name,
        c.entity_type                               AS counterparty_entity_type,
        c.country_of_incorporation                  AS counterparty_country,
        c.risk_rating                               AS counterparty_risk_rating,

        -- Sanctions entity context
        sl.entity_name                              AS sanctioned_entity_name,
        sl.sanctions_list                           AS sanctioned_entity_list,
        sl.sanctions_program                        AS sanctioned_entity_program,
        sl.is_active                                AS sanctioned_entity_is_active,

        -- SLA compliance based on risk level
        CASE
            WHEN s.risk_level = 'CRITICAL' AND s.resolution_hours <= 4 THEN TRUE
            WHEN s.risk_level = 'HIGH' AND s.resolution_hours <= 24 THEN TRUE
            WHEN s.risk_level = 'MEDIUM' AND s.resolution_hours <= 48 THEN TRUE
            WHEN s.risk_level = 'LOW' AND s.resolution_hours <= 72 THEN TRUE
            WHEN s.resolution_hours IS NULL THEN NULL  -- Still pending
            ELSE FALSE
        END                                         AS is_within_sla,

        -- Derived flags
        CASE WHEN s.disposition = 'TRUE_POSITIVE' THEN TRUE ELSE FALSE END AS is_true_positive,
        CASE WHEN s.disposition = 'FALSE_POSITIVE' THEN TRUE ELSE FALSE END AS is_false_positive

    FROM screening s
    LEFT JOIN counterparties c ON s.counterparty_id = c.counterparty_id
    LEFT JOIN sanctions_lists sl ON s.sanctioned_entity_id = sl.entity_id

)

SELECT * FROM enriched
