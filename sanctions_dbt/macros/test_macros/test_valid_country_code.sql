{% test valid_country_code(model, column_name) %}
{#
    Custom test validating column contains valid ISO 3166-1 alpha-2 country codes.
    Cross-references seed_country_codes seed table.
#}

SELECT {{ column_name }}
FROM {{ model }}
WHERE {{ column_name }} IS NOT NULL
  AND {{ column_name }} NOT IN (
      SELECT country_code FROM {{ ref('seed_country_codes') }}
  )

{% endtest %}
