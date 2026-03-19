{{
    config(
        materialized='ephemeral'
    )
}}

/*
    Builds vessel risk profile from movement patterns.
    Aggregates dark activity, sanctioned zone visits, and risk scores.
    Derives vessel_risk_score and vessel_risk_tier.
*/

WITH vessels AS (

    SELECT * FROM {{ ref('stg_vessels') }}

),

movements AS (

    SELECT * FROM {{ ref('stg_vessel_movements') }}

),

movement_stats AS (

    SELECT
        vessel_id,
        COUNT(*)                                    AS total_movements,
        COUNT_IF(is_dark_activity)                  AS dark_activity_count,
        SUM(COALESCE(dark_duration_hours, 0))       AS total_dark_hours,
        COUNT_IF(is_near_sanctioned_zone)           AS sanctioned_zone_visits,
        AVG(zone_risk_score)                        AS avg_zone_risk_score,
        MAX(zone_risk_score)                        AS max_zone_risk_score,
        MIN(movement_timestamp)                     AS first_movement_at,
        MAX(movement_timestamp)                     AS last_movement_at,
        COUNT(DISTINCT voyage_id)                   AS total_voyages

    FROM movements
    GROUP BY vessel_id

),

risk_profile AS (

    SELECT
        v.vessel_id,
        v.imo_number,
        v.vessel_name,
        v.vessel_type,
        v.flag_state,
        v.is_flagged,
        v.registered_owner,
        v.beneficial_owner,
        v.year_built,

        COALESCE(ms.total_movements, 0)             AS total_movements,
        COALESCE(ms.dark_activity_count, 0)         AS dark_activity_count,
        COALESCE(ms.total_dark_hours, 0)            AS total_dark_hours,
        COALESCE(ms.sanctioned_zone_visits, 0)      AS sanctioned_zone_visits,
        COALESCE(ms.avg_zone_risk_score, 0)         AS avg_zone_risk_score,
        COALESCE(ms.max_zone_risk_score, 0)         AS max_zone_risk_score,
        ms.first_movement_at,
        ms.last_movement_at,
        COALESCE(ms.total_voyages, 0)               AS total_voyages,

        -- Composite vessel risk score (0-100)
        CASE
            WHEN v.is_flagged THEN 90
            WHEN ms.dark_activity_count > 0 AND ms.sanctioned_zone_visits > 0 THEN 85
            WHEN ms.dark_activity_count > 5 OR ms.sanctioned_zone_visits > 3 THEN 70
            WHEN ms.dark_activity_count > 0 OR ms.sanctioned_zone_visits > 0 THEN 50
            WHEN COALESCE(ms.avg_zone_risk_score, 0) > 30 THEN 40
            ELSE 10
        END                                         AS vessel_risk_score,

        -- Risk tier classification
        CASE
            WHEN v.is_flagged THEN 'CRITICAL'
            WHEN ms.dark_activity_count > 0 AND ms.sanctioned_zone_visits > 0 THEN 'CRITICAL'
            WHEN ms.dark_activity_count > 5 OR ms.sanctioned_zone_visits > 3 THEN 'HIGH'
            WHEN ms.dark_activity_count > 0 OR ms.sanctioned_zone_visits > 0 THEN 'MEDIUM'
            ELSE 'LOW'
        END                                         AS vessel_risk_tier

    FROM vessels v
    LEFT JOIN movement_stats ms ON v.vessel_id = ms.vessel_id

)

SELECT * FROM risk_profile
