{{
    config(
        materialized='view',
        tags=['staging', 'daily']
    )
}}

/*
    Staging model: Screening Results
    Source: RAW.RAW_SCREENING_RESULTS
    Cleanse and standardize sanctions screening results.
*/

WITH source AS (

    SELECT * FROM {{ source('raw', 'raw_screening_results') }}

),

renamed AS (

    SELECT
        -- Primary Key
        screening_id,

        -- Foreign keys
        trade_id,
        counterparty_id,
        sanctioned_entity_id,

        -- Temporal
        screening_timestamp::TIMESTAMP_NTZ              AS screening_timestamp,

        -- Screening classification
        UPPER(TRIM(screening_type))                     AS screening_type,

        -- Match details
        COALESCE(match_score, 0)::NUMBER(6,4)           AS match_score,
        UPPER(TRIM(match_type))                         AS match_type,
        TRY_PARSE_JSON(match_details)                   AS match_details,

        -- Disposition
        UPPER(TRIM(disposition))                        AS disposition,
        UPPER(TRIM(risk_level))                         AS risk_level,

        -- Analyst
        TRIM(analyst_id)                                AS analyst_id,
        analyst_notes,

        -- Resolution
        resolution_timestamp::TIMESTAMP_NTZ             AS resolution_timestamp,
        resolution_hours::NUMBER(8,2)                   AS resolution_hours,

        -- Sanctions reference
        UPPER(TRIM(sanctions_list_matched))             AS sanctions_list_matched,
        COALESCE(is_pep_match, FALSE)                   AS is_pep_match,
        COALESCE(is_adverse_media, FALSE)               AS is_adverse_media,

        -- Workflow
        TRIM(workflow_id)                               AS workflow_id,
        UPPER(TRIM(source_system))                      AS source_system,

        -- Metadata
        _loaded_at,
        {{ audit_columns() }}

    FROM source

)

SELECT * FROM renamed
