WITH source AS (

    SELECT * FROM {{ source('raw', 'raw_sanctions_lists') }}

),

renamed AS (

    SELECT
        entity_id,
        TRIM(entity_name)                           AS entity_name,
        UPPER(TRIM(entity_type))                    AS entity_type,
        UPPER(TRIM(sanctions_list))                 AS sanctions_list,
        TRIM(sanctions_program)                     AS sanctions_program,
        UPPER(TRIM(nationality))                    AS nationality,
        TRY_PARSE_JSON(country_codes)               AS country_codes,
        TRY_PARSE_JSON(alias_names)                 AS alias_names,
        TRY_PARSE_JSON(identification_numbers)      AS identification_numbers,
        listed_date::DATE                           AS listed_date,
        delisted_date::DATE                         AS delisted_date,
        last_updated::TIMESTAMP_NTZ                 AS last_updated,
        TRIM(remarks)                               AS remarks,
        TRIM(source_url)                            AS source_url,
        TRIM(source_system)                         AS source_system,
        _loaded_at,

        -- Derived: is this entity currently active on the list?
        CASE
            WHEN delisted_date IS NULL THEN TRUE
            WHEN delisted_date > CURRENT_DATE() THEN TRUE
            ELSE FALSE
        END                                         AS is_active

    FROM source

)

SELECT * FROM renamed
