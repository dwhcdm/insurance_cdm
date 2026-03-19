{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        tags=['marts', 'core', 'hourly'],
        cluster_by=['movement_timestamp::DATE', 'vessel_id'],
        unique_key='movement_id'
    )
}}

/*
    Fact: Vessel Movements (AIS Data)
    THE BILLIONS TABLE - incremental append for cost efficiency.
    3.65B+ records at production scale.
*/

WITH movements AS (

    SELECT * FROM {{ ref('stg_vessel_movements') }}

    {% if is_incremental() %}
    WHERE _loaded_at > (SELECT COALESCE(MAX(_loaded_at), '1900-01-01') FROM {{ this }})
    {% endif %}

),

final AS (

    SELECT
        -- Primary key
        movement_id,

        -- Vessel FK
        vessel_id,
        {{ surrogate_key(['vessel_id']) }}               AS vessel_sk,

        -- Position
        movement_timestamp,
        latitude,
        longitude,
        speed_knots,
        heading,

        -- Ports
        port_of_call,
        origin_port,
        destination_port,
        voyage_id,

        -- Dark activity
        is_dark_activity,
        dark_duration_hours,

        -- Risk
        zone_risk_score,
        is_near_sanctioned_zone,

        -- AIS
        ais_message_type,
        navigation_status,
        source_system,

        -- Metadata
        _loaded_at,
        _dbt_loaded_at

    FROM movements

)

SELECT * FROM final
