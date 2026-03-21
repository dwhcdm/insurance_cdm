WITH source AS (

    SELECT * FROM {{ source('raw', 'raw_vessels') }}

),

renamed AS (

    SELECT
        vessel_id,
        TRIM(imo_number)                            AS imo_number,
        TRIM(mmsi)                                  AS mmsi,
        TRIM(vessel_name)                           AS vessel_name,
        UPPER(TRIM(call_sign))                      AS call_sign,
        UPPER(TRIM(vessel_type))                    AS vessel_type,
        UPPER(TRIM(flag_state))                     AS flag_state,
        TRIM(class_society)                         AS class_society,
        dwt::NUMBER(12,2)                           AS dwt,
        gross_tonnage::NUMBER(12,2)                 AS gross_tonnage,
        year_built::INTEGER                         AS year_built,
        TRIM(builder)                               AS builder,
        UPPER(TRIM(status))                         AS status,
        is_flagged,
        TRIM(registered_owner)                      AS registered_owner,
        TRIM(beneficial_owner)                      AS beneficial_owner,
        TRIM(source_system)                         AS source_system,
        _loaded_at

    FROM source

)

SELECT * FROM renamed
