{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        unique_key='movement_id',
        cluster_by=['movement_timestamp::DATE', 'vessel_id']
    )
}}

/*
    Fact: AIS vessel movements - incremental append.
    3.65B+ records at production scale.
*/

WITH movements AS (

    SELECT * FROM {{ ref('stg_vessel_movements') }}
    {% if is_incremental() %}
    WHERE _loaded_at > (SELECT MAX(_loaded_at) FROM {{ this }})
    {% endif %}

),

final AS (

    SELECT
        {{ hash_surrogate_key(['m.vessel_id']) }}   AS vessel_sk,

        m.movement_id,
        m.vessel_id,
        m.movement_timestamp,
        m.latitude,
        m.longitude,
        m.speed_knots,
        m.heading,
        m.port_of_call,
        m.origin_port,
        m.destination_port,
        m.voyage_id,
        m.is_dark_activity,
        m.dark_duration_hours,
        m.zone_risk_score,
        m.is_near_sanctioned_zone,
        m.ais_message_type,
        m.navigation_status,
        m.source_system,
        m._loaded_at,

        {{ audit_columns() }}

    FROM movements m

)

SELECT * FROM final
