{% macro grant_select_on_schemas(schemas, role) %}
    {#
        Grant SELECT on all tables and views in specified schemas to a role.
        Used as a post-hook on mart models to ensure analysts have access.

        Args:
            schemas: List of schema names to grant access to.
            role: The Snowflake role to grant access to.
    #}
    {% for schema_name in schemas %}
        GRANT USAGE ON SCHEMA {{ target.database }}.{{ schema_name }} TO ROLE {{ role }};
        GRANT SELECT ON ALL TABLES IN SCHEMA {{ target.database }}.{{ schema_name }} TO ROLE {{ role }};
        GRANT SELECT ON ALL VIEWS IN SCHEMA {{ target.database }}.{{ schema_name }} TO ROLE {{ role }};
        GRANT SELECT ON FUTURE TABLES IN SCHEMA {{ target.database }}.{{ schema_name }} TO ROLE {{ role }};
        GRANT SELECT ON FUTURE VIEWS IN SCHEMA {{ target.database }}.{{ schema_name }} TO ROLE {{ role }};
    {% endfor %}
{% endmacro %}
