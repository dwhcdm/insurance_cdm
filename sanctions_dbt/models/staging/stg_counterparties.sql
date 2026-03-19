{{
    config(
        materialized='view',
        tags=['staging', 'daily']
    )
}}

/*
    Staging model: Counterparties
    Source: RAW.RAW_COUNTERPARTIES
    Cleanse, type-cast, and standardize counterparty/KYC data.
*/

WITH source AS (

    SELECT * FROM {{ source('raw', 'raw_counterparties') }}

),

renamed AS (

    SELECT
        -- Primary Key
        counterparty_id,

        -- Entity attributes
        UPPER(TRIM(legal_name))                         AS legal_name,
        UPPER(TRIM(entity_type))                        AS entity_type,
        UPPER(TRIM(country_of_incorporation))           AS country_of_incorporation,
        UPPER(TRIM(country_of_domicile))                AS country_of_domicile,
        TRIM(registration_number)                       AS registration_number,
        TRIM(lei_code)                                  AS lei_code,
        TRIM(swift_bic)                                 AS swift_bic,
        TRIM(tax_id)                                    AS tax_id,

        -- Address
        TRIM(address_line_1)                            AS address_line_1,
        TRIM(address_line_2)                            AS address_line_2,
        TRIM(city)                                      AS city,
        UPPER(TRIM(state_province))                     AS state_province,
        TRIM(postal_code)                               AS postal_code,

        -- Classification
        UPPER(TRIM(industry_sector))                    AS industry_sector,
        UPPER(TRIM(risk_rating))                        AS risk_rating,
        COALESCE(is_pep, FALSE)                         AS is_pep,
        COALESCE(is_sanctioned, FALSE)                  AS is_sanctioned,

        -- Semi-structured data
        TRY_PARSE_JSON(alias_names)                     AS alias_names,

        -- Dates
        registration_date::DATE                         AS registration_date,
        last_kyc_review_date::DATE                      AS last_kyc_review_date,

        -- Metadata
        source_system,
        _loaded_at,
        {{ audit_columns() }}

    FROM source

)

SELECT * FROM renamed
