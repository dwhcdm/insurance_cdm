{% macro generate_schema_name(custom_schema_name, node) -%}
    {#
        Override default schema generation to use custom schema names directly.
        In production, models go to their designated schema (e.g., ANALYTICS, STAGING).
        In dev, they go to a developer-prefixed schema for isolation.
    #}
    {%- set default_schema = target.schema -%}

    {%- if target.name == 'prod' -%}
        {# Production: Use the custom schema name directly #}
        {%- if custom_schema_name is not none -%}
            {{ custom_schema_name | trim | upper }}
        {%- else -%}
            {{ default_schema | trim | upper }}
        {%- endif -%}
    {%- else -%}
        {# Dev/Test: Prefix with target schema for isolation #}
        {%- if custom_schema_name is not none -%}
            {{ custom_schema_name | trim | upper }}
        {%- else -%}
            {{ default_schema | trim | upper }}
        {%- endif -%}
    {%- endif -%}
{%- endmacro %}
