WITH source AS (

    SELECT * FROM {{ source('raw', 'raw_counterparties') }}

),

renamed AS (

    SELECT
        counterparty_id,
        TRIM(legal_name)                            AS legal_name,
        UPPER(TRIM(entity_type))                    AS entity_type,
        UPPER(TRIM(country_of_incorporation))       AS country_of_incorporation,
        UPPER(TRIM(country_of_domicile))            AS country_of_domicile,
        TRIM(registration_number)                   AS registration_number,
        TRIM(lei_code)                              AS lei_code,
        UPPER(TRIM(swift_bic))                      AS swift_bic,
        TRIM(tax_id)                                AS tax_id,
        TRIM(address_line_1)                        AS address_line_1,
        TRIM(address_line_2)                        AS address_line_2,
        TRIM(city)                                  AS city,
        TRIM(state_province)                        AS state_province,
        TRIM(postal_code)                           AS postal_code,
        TRIM(industry_sector)                       AS industry_sector,
        UPPER(TRIM(risk_rating))                    AS risk_rating,
        is_pep,
        is_sanctioned,
        TRY_PARSE_JSON(alias_names)                 AS alias_names,
        registration_date::DATE                     AS registration_date,
        last_kyc_review_date::DATE                  AS last_kyc_review_date,
        TRIM(source_system)                         AS source_system,
        _loaded_at

    FROM source

)

SELECT * FROM renamed
