{{
    config(
        materialized='table',
        cluster_by=['vessel_type', 'flag_state']
    )
}}

/*
    Dimension: Vessels with risk profile from movement patterns.
    Includes dark activity metrics and vessel age classification.
*/

WITH vessel_risk AS (

    SELECT * FROM {{ ref('int_vessel_risk_profile') }}

),

final AS (

    SELECT
        {{ hash_surrogate_key(['vr.vessel_id']) }}  AS vessel_sk,
        vr.vessel_id,
        vr.imo_number,
        vr.vessel_name,
        vr.vessel_type,
        vr.flag_state,
        vr.is_flagged,
        vr.registered_owner,
        vr.beneficial_owner,
        vr.year_built,

        -- Movement statistics
        vr.total_movements,
        vr.dark_activity_count,
        vr.total_dark_hours,
        vr.sanctioned_zone_visits,
        vr.avg_zone_risk_score,
        vr.max_zone_risk_score,
        vr.first_movement_at,
        vr.last_movement_at,
        vr.total_voyages,

        -- Risk classification
        vr.vessel_risk_score,
        vr.vessel_risk_tier,

        -- Vessel age classification
        CASE
            WHEN YEAR(CURRENT_DATE()) - vr.year_built <= 5 THEN 'NEW'
            WHEN YEAR(CURRENT_DATE()) - vr.year_built <= 15 THEN 'MODERN'
            WHEN YEAR(CURRENT_DATE()) - vr.year_built <= 25 THEN 'AGING'
            ELSE 'OLD'
        END                                         AS vessel_age_class,

        {{ audit_columns() }}

    FROM vessel_risk vr

)

SELECT * FROM final
