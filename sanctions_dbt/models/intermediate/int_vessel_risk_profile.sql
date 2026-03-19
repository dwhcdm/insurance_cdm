{{
    config(
        materialized='ephemeral'
    )
}}

/*
    Intermediate: Build vessel risk profile by combining vessel master data
    with movement patterns and dark activity history.
*/

WITH vessels AS (

    SELECT * FROM {{ ref('stg_vessels') }}

),

movements_agg AS (

    SELECT
        vessel_id,
        COUNT(*)                                        AS total_movements,
        COUNT_IF(is_dark_activity)                      AS dark_activity_count,
        SUM(COALESCE(dark_duration_hours, 0))           AS total_dark_hours,
        COUNT_IF(is_near_sanctioned_zone)               AS sanctioned_zone_visits,
        AVG(zone_risk_score)                            AS avg_zone_risk_score,
        MAX(zone_risk_score)                            AS max_zone_risk_score,
        MIN(movement_timestamp)                         AS first_movement_date,
        MAX(movement_timestamp)                         AS last_movement_date,
        COUNT(DISTINCT origin_port)                     AS distinct_origin_ports,
        COUNT(DISTINCT destination_port)                AS distinct_destination_ports

    FROM {{ ref('stg_vessel_movements') }}
    GROUP BY vessel_id

),

risk_profile AS (

    SELECT
        v.vessel_id,
        v.imo_number,
        v.vessel_name,
        v.vessel_type,
        v.flag_state,
        v.deadweight_tonnage,
        v.year_built,
        v.vessel_status,
        v.is_flagged,
        v.registered_owner,
        v.beneficial_owner,

        -- Movement statistics
        COALESCE(m.total_movements, 0)                  AS total_movements,
        COALESCE(m.dark_activity_count, 0)              AS dark_activity_count,
        COALESCE(m.total_dark_hours, 0)                 AS total_dark_hours,
        COALESCE(m.sanctioned_zone_visits, 0)           AS sanctioned_zone_visits,
        COALESCE(m.avg_zone_risk_score, 0)              AS avg_zone_risk_score,
        COALESCE(m.max_zone_risk_score, 0)              AS max_zone_risk_score,
        m.first_movement_date,
        m.last_movement_date,
        COALESCE(m.distinct_origin_ports, 0)            AS distinct_origin_ports,
        COALESCE(m.distinct_destination_ports, 0)       AS distinct_destination_ports,

        -- Composite risk score
        CASE
            WHEN v.is_flagged THEN 90
            WHEN m.dark_activity_count > 10 AND m.sanctioned_zone_visits > 5 THEN 85
            WHEN m.dark_activity_count > 5 OR m.sanctioned_zone_visits > 3 THEN 70
            WHEN m.dark_activity_count > 0 OR m.sanctioned_zone_visits > 0 THEN 50
            WHEN m.avg_zone_risk_score > 30 THEN 40
            ELSE 10
        END                                             AS vessel_risk_score,

        -- Risk tier
        CASE
            WHEN v.is_flagged THEN 'CRITICAL'
            WHEN m.dark_activity_count > 10 AND m.sanctioned_zone_visits > 5 THEN 'CRITICAL'
            WHEN m.dark_activity_count > 5 OR m.sanctioned_zone_visits > 3 THEN 'HIGH'
            WHEN m.dark_activity_count > 0 OR m.sanctioned_zone_visits > 0 THEN 'MEDIUM'
            ELSE 'LOW'
        END                                             AS vessel_risk_tier

    FROM vessels v
    LEFT JOIN movements_agg m
        ON v.vessel_id = m.vessel_id

)

SELECT * FROM risk_profile
