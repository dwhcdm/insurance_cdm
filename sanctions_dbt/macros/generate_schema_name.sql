{% macro generate_schema_name(custom_schema_name, node) -%}
    {#
        Override default schema generation.
        In all environments, use the custom schema name directly
        so models go to their designated schemas (ANALYTICS, STAGING, etc.)
    #}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
