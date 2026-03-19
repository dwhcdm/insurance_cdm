{{
    config(
        materialized='table',
        tags=['marts', 'core', 'daily'],
        cluster_by=['screening_timestamp::DATE', 'disposition']
    )
}}

/*
    Fact: Screening Results
    Sanctions screening results enriched with counterparty and entity context.
*/

WITH enriched AS (

    SELECT * FROM {{ ref('int_screening_enriched') }}

),

final AS (

    SELECT
        {{ surrogate_key(['screening_id']) }}            AS screening_sk,

        -- Keys
        screening_id,
        trade_id,
        {{ surrogate_key(['counterparty_id']) }}         AS counterparty_sk,
        {{ surrogate_key(['sanctioned_entity_id']) }}    AS sanctions_sk,

        -- Natural keys
        counterparty_id,
        sanctioned_entity_id,

        -- Temporal
        screening_timestamp,
        resolution_timestamp,

        -- Classification
        screening_type,
        match_score,
        match_type,
        match_details,
        disposition,
        risk_level,

        -- Analyst
        analyst_id,
        analyst_notes,

        -- Measures
        resolution_hours,

        -- SLA
        is_within_sla,
        is_true_positive,
        is_false_positive,

        -- Context
        counterparty_name,
        counterparty_entity_type,
        counterparty_country,
        counterparty_risk_rating,
        sanctioned_entity_name,
        sanctioned_entity_type,
        sanctioned_list_source,
        sanctions_program,
        sanctioned_entity_is_active,

        -- Sanctions list
        sanctions_list_matched,
        is_pep_match,
        is_adverse_media,

        -- Workflow
        workflow_id,
        source_system,

        _dbt_loaded_at

    FROM enriched

)

SELECT * FROM final
