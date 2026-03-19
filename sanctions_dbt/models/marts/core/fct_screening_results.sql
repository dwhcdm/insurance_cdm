{{
    config(
        materialized='table',
        cluster_by=['screening_timestamp::DATE', 'disposition']
    )
}}

/*
    Fact: Screening results enriched with counterparty and entity context.
    Includes SLA tracking and derived flags.
*/

WITH screening AS (

    SELECT * FROM {{ ref('int_screening_enriched') }}

),

final AS (

    SELECT
        {{ hash_surrogate_key(['s.screening_id']) }}            AS screening_sk,
        {{ hash_surrogate_key(['s.counterparty_id']) }}         AS counterparty_sk,
        {{ hash_surrogate_key(['s.sanctioned_entity_id']) }}    AS sanctions_entity_sk,

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

        -- Enriched context
        s.counterparty_name,
        s.counterparty_entity_type,
        s.counterparty_country,
        s.counterparty_risk_rating,
        s.sanctioned_entity_name,
        s.sanctioned_entity_list,
        s.sanctioned_entity_program,
        s.sanctioned_entity_is_active,

        -- Derived flags
        s.is_within_sla,
        s.is_true_positive,
        s.is_false_positive,

        {{ audit_columns() }}

    FROM screening s

)

SELECT * FROM final
