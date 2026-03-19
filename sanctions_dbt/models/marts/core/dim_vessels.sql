{{
    config(
        materialized='table',
        tags=['marts', 'core', 'daily'],
        cluster_by=['vessel_type', 'flag_state']
    )
}}

/*
    Dimension: Vessels
    Enriched vessel dimension with risk profile from movement analysis.
*/

WITH vessel_risk AS (

    SELECT * FROM {{ ref('int_vessel_risk_profile') }}

),

final AS (

    SELECT
        {{ surrogate_key(['vessel_id']) }}               AS vessel_sk,
        vessel_id,
        imo_number,
        vessel_name,
        vessel_type,
        flag_state,
        class_society,
        deadweight_tonnage,
        gross_tonnage,
        year_built,
        builder,
        vessel_status,
        is_flagged,
        registered_owner,
        beneficial_owner,

        -- Movement stats
        total_movements,
        dark_activity_count,
        total_dark_hours,
        sanctioned_zone_visits,
        avg_zone_risk_score,
        max_zone_risk_score,
        first_movement_date,
        last_movement_date,
        distinct_origin_ports,
        distinct_destination_ports,

        -- Derived risk
        vessel_risk_score,
        vessel_risk_tier,

        -- Age classification
        CASE
            WHEN YEAR(CURRENT_DATE()) - year_built <= 5  THEN 'NEW'
            WHEN YEAR(CURRENT_DATE()) - year_built <= 15 THEN 'MODERN'
            WHEN YEAR(CURRENT_DATE()) - year_built <= 25 THEN 'AGING'
            ELSE 'OLD'
        END                                             AS vessel_age_class,

        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ              AS _dbt_loaded_at

    FROM vessel_risk

)

SELECT * FROM final
