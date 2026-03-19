WITH source AS (

    SELECT * FROM {{ source('raw', 'raw_screening_results') }}

),

renamed AS (

    SELECT
        screening_id,
        trade_id,
        counterparty_id,
        sanctioned_entity_id,
        screening_timestamp::TIMESTAMP_NTZ          AS screening_timestamp,
        UPPER(TRIM(screening_type))                 AS screening_type,
        match_score::NUMBER(6,4)                    AS match_score,
        UPPER(TRIM(match_type))                     AS match_type,
        TRY_PARSE_JSON(match_details)               AS match_details,
        UPPER(TRIM(disposition))                    AS disposition,
        UPPER(TRIM(risk_level))                     AS risk_level,
        TRIM(analyst_id)                            AS analyst_id,
        TRIM(analyst_notes)                         AS analyst_notes,
        resolution_timestamp::TIMESTAMP_NTZ         AS resolution_timestamp,
        resolution_hours::NUMBER(8,2)               AS resolution_hours,
        UPPER(TRIM(sanctions_list_matched))         AS sanctions_list_matched,
        is_pep_match,
        is_adverse_media,
        TRIM(workflow_id)                           AS workflow_id,
        TRIM(source_system)                         AS source_system,
        _loaded_at

    FROM source

)

SELECT * FROM renamed
