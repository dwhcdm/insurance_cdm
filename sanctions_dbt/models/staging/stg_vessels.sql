{{
    config(
        materialized='view',
        tags=['staging', 'daily']
    )
}}

/*
    Staging model: Vessels
    Source: RAW.RAW_VESSELS
    Cleanse and standardize vessel master data.
*/

WITH source AS (

    SELECT * FROM {{ source('raw', 'raw_vessels') }}

),

renamed AS (

    SELECT
        -- Primary Key
        vessel_id,

        -- Vessel identification
        TRIM(imo_number)                                AS imo_number,
        TRIM(mmsi)                                      AS mmsi,
        UPPER(TRIM(vessel_name))                        AS vessel_name,
        UPPER(TRIM(call_sign))                          AS call_sign,

        -- Classification
        UPPER(TRIM(vessel_type))                        AS vessel_type,
        UPPER(TRIM(flag_state))                         AS flag_state,
        UPPER(TRIM(class_society))                      AS class_society,

        -- Technical specs
        COALESCE(dwt, 0)::NUMBER(12,2)                  AS deadweight_tonnage,
        COALESCE(gross_tonnage, 0)::NUMBER(12,2)        AS gross_tonnage,
        year_built::INTEGER                             AS year_built,
        UPPER(TRIM(builder))                            AS builder,

        -- Status
        UPPER(TRIM(status))                             AS vessel_status,
        COALESCE(is_flagged, FALSE)                     AS is_flagged,

        -- Ownership
        TRIM(registered_owner)                          AS registered_owner,
        TRIM(beneficial_owner)                          AS beneficial_owner,

        -- Metadata
        source_system,
        _loaded_at,
        {{ audit_columns() }}

    FROM source

)

SELECT * FROM renamed
