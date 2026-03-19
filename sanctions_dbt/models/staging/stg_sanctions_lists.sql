{{
    config(
        materialized='view',
        tags=['staging', 'daily']
    )
}}

/*
    Staging model: Sanctions Lists
    Source: RAW.RAW_SANCTIONS_LISTS
    Cleanse and standardize sanctions list entries from OFAC/EU/UN/UK.
*/

WITH source AS (

    SELECT * FROM {{ source('raw', 'raw_sanctions_lists') }}

),

renamed AS (

    SELECT
        -- Primary Key
        entity_id                                       AS sanctioned_entity_id,

        -- Entity attributes
        UPPER(TRIM(entity_name))                        AS entity_name,
        UPPER(TRIM(entity_type))                        AS entity_type,

        -- Sanctions details
        UPPER(TRIM(sanctions_list))                     AS sanctions_list,
        UPPER(TRIM(sanctions_program))                  AS sanctions_program,

        -- Jurisdiction & nationality
        UPPER(TRIM(nationality))                        AS nationality,

        -- Semi-structured data
        TRY_PARSE_JSON(country_codes)                   AS country_codes,
        TRY_PARSE_JSON(alias_names)                     AS alias_names,
        TRY_PARSE_JSON(identification_numbers)          AS identification_numbers,

        -- Dates
        listed_date::DATE                               AS listed_date,
        delisted_date::DATE                             AS delisted_date,
        last_updated::TIMESTAMP_NTZ                     AS last_updated,

        -- Derived flags
        CASE
            WHEN delisted_date IS NULL OR delisted_date > CURRENT_DATE()
            THEN TRUE
            ELSE FALSE
        END                                             AS is_active,

        -- Additional details
        remarks,
        source_url,
        source_system,

        -- Metadata
        _loaded_at,
        {{ audit_columns() }}

    FROM source

)

SELECT * FROM renamed
