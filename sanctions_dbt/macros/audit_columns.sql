{% macro audit_columns() %}
    {#
        Standard audit columns added to all materialized models.
        Provides lineage tracking for every row.
    #}
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS _dbt_loaded_at,
    '{{ invocation_id }}' AS _dbt_invocation_id
{% endmacro %}
