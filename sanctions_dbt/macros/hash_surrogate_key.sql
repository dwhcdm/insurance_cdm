{% macro surrogate_key(field_list) %}
    {#
        Generate a deterministic surrogate key using MD5 hash.
        Handles NULLs by coalescing to a placeholder string.

        Args:
            field_list: List of column expressions to hash.

        Returns:
            MD5 hash string suitable for use as a surrogate key.
    #}
    MD5(
        {%- for field in field_list %}
            COALESCE(CAST({{ field }} AS VARCHAR), '_SANCTIONS_NULL_')
            {%- if not loop.last %} || '||' || {% endif -%}
        {% endfor -%}
    )
{% endmacro %}
