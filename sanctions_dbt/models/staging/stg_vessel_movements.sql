WITH source AS (

    SELECT * FROM {{ source('raw', 'raw_vessel_movements') }}

),

renamed AS (

    SELECT
        movement_id,
        vessel_id,
        timestamp::TIMESTAMP_NTZ                    AS movement_timestamp,
        latitude::NUMBER(10,6)                      AS latitude,
        longitude::NUMBER(11,6)                     AS longitude,
        speed_knots::NUMBER(6,1)                    AS speed_knots,
        heading::NUMBER(5,1)                        AS heading,
        TRIM(port_of_call)                          AS port_of_call,
        TRIM(origin_port)                           AS origin_port,
        TRIM(destination_port)                      AS destination_port,
        TRIM(voyage_id)                             AS voyage_id,
        is_dark_activity,
        dark_duration_hours::NUMBER(6,1)            AS dark_duration_hours,
        zone_risk_score::NUMBER(5,2)                AS zone_risk_score,
        is_near_sanctioned_zone,
        ais_message_type,
        TRIM(navigation_status)                     AS navigation_status,
        TRIM(source_system)                         AS source_system,
        _loaded_at

    FROM source

)

SELECT * FROM renamed
