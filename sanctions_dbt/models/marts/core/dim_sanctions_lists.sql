{{
    config(
        materialized='table'
    )
}}

/*
    Dimension: Sanctions list reference data with derived days_on_list.
*/

WITH sanctions AS (

    SELECT * FROM {{ ref('stg_sanctions_lists') }}

),

final AS (

    SELECT
        {{ hash_surrogate_key(['entity_id']) }}     AS sanctions_entity_sk,
        entity_id,
        entity_name,
        entity_type,
        sanctions_list,
        sanctions_program,
        nationality,
        country_codes,
        alias_names,
        identification_numbers,
        listed_date,
        delisted_date,
        last_updated,
        remarks,
        source_url,
        source_system,
        is_active,

        -- Derived: days on the list
        DATEDIFF('day', listed_date,
            COALESCE(delisted_date, CURRENT_DATE())
        )                                           AS days_on_list,

        {{ audit_columns() }}

    FROM sanctions

)

SELECT * FROM final
