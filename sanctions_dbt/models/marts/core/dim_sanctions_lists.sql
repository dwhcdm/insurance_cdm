{{
    config(
        materialized='table',
        tags=['marts', 'core', 'daily']
    )
}}

/*
    Dimension: Sanctions Lists
    Reference dimension for all sanctioned entities across lists.
*/

WITH sanctions AS (

    SELECT * FROM {{ ref('stg_sanctions_lists') }}

),

final AS (

    SELECT
        {{ surrogate_key(['sanctioned_entity_id']) }}    AS sanctions_sk,
        sanctioned_entity_id,
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
        is_active,
        remarks,
        source_url,
        source_system,

        -- Duration on list
        DATEDIFF('day', listed_date, COALESCE(delisted_date, CURRENT_DATE()))
                                                        AS days_on_list,

        _dbt_loaded_at

    FROM sanctions

)

SELECT * FROM final
