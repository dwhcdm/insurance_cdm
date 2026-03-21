{% macro hash_surrogate_key(field_list) %}
    {#
        Generate deterministic surrogate key using MD5 hash.
        Handles NULLs by coalescing to '_SANCTIONS_NULL_'.
    #}
    MD5(
        {% for field in field_list %}
            COALESCE(CAST({{ field }} AS VARCHAR), '_SANCTIONS_NULL_')
            {% if not loop.last %} || '||' || {% endif %}
        {% endfor %}
    )
{% endmacro %}
