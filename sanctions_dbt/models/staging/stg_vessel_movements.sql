{{
    config(
        materialized='view',
        tags=['staging', 'daily']
    )
}}

/*
    Staging model: Vessel Movements (AIS Data)
    Source: RAW.RAW_VESSEL_MOVEMENTS
    THE BILLIONS TABLE - 3.65B+ records at production scale.
*/

WITH source AS (

    SELECT * FROM {{ source('raw', 'raw_vessel_movements') }}

),

renamed AS (

    SELECT
        -- Primary Key
        movement_id,

        -- Vessel FK
        vessel_id,

        -- Position & time
        timestamp::TIMESTAMP_NTZ                        AS movement_timestamp,
        latitude::NUMBER(10,6)                          AS latitude,
        longitude::NUMBER(11,6)                         AS longitude,
        COALESCE(speed_knots, 0)::NUMBER(6,1)           AS speed_knots,
        COALESCE(heading, 0)::NUMBER(5,1)               AS heading,

        -- Port information
        UPPER(TRIM(port_of_call))                       AS port_of_call,
        UPPER(TRIM(origin_port))                        AS origin_port,
        UPPER(TRIM(destination_port))                   AS destination_port,
        TRIM(voyage_id)                                 AS voyage_id,

        -- Dark activity flags
        COALESCE(is_dark_activity, FALSE)               AS is_dark_activity,
        dark_duration_hours::NUMBER(6,1)                AS dark_duration_hours,

        -- Risk indicators
        COALESCE(zone_risk_score, 0)::NUMBER(5,2)       AS zone_risk_score,
        COALESCE(is_near_sanctioned_zone, FALSE)        AS is_near_sanctioned_zone,

        -- AIS specifics
        ais_message_type::INTEGER                       AS ais_message_type,
        UPPER(TRIM(navigation_status))                  AS navigation_status,

        -- Source
        UPPER(TRIM(source_system))                      AS source_system,

        -- Metadata
        _loaded_at,
        {{ audit_columns() }}

    FROM source

)

SELECT * FROM renamed
