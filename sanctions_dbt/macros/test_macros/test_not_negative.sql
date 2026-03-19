{% test not_negative(model, column_name) %}
{#
    Custom test: Ensures a numeric column contains no negative values.
    Useful for financial measures like total_value_usd, quantity_mt.
#}

SELECT {{ column_name }}
FROM {{ model }}
WHERE {{ column_name }} < 0

{% endtest %}
