{% macro snowflake__create_external_schema(source_node) %}

    {% set schema_exists_query %}
        show terse schemas like '{{ source_node.schema }}' in database {{ source_node.database }} limit 1;
    {% endset %}
    {% if execute %}
        {% set schema_exists = run_query(schema_exists_query)|length > 0 %}
    {% else %}
        {% set schema_exists = false %}
    {% endif %}

    {% if schema_exists %}
        {% set ddl %}
            select 'Schema {{ source_node.schema }} exists' from dual;
        {% endset %}
    {% else %}
        {% set fqn %}
            {% if source_node.database %}
                {{ source_node.database }}.{{ source_node.schema }}
            {% else %}
                {{ source_node.schema }}
            {% endif %}
        {% endset %}

        {% set ddl %}
            create schema if not exists {{ fqn }};
        {% endset %}
    {% endif %}

    {% do return(ddl) %}

{% endmacro %}

{% macro snowflake__refresh_external_table(source_node) %}

    {% set external = source_node.external %}
    {% set snowpipe = source_node.external.get('snowpipe', none) %}

    {% set auto_refresh = external.get('auto_refresh', false) %}
    {% set partitions = external.get('partitions', none) %}
    {% set delta_format = (external.table_format | lower == "delta") %}

    {% set manual_refresh = not auto_refresh %}

    {% if manual_refresh %}

        {% set ddl %}
        begin;
        alter external table {{source(source_node.source_name, source_node.name)}} refresh;
        commit;
        {% endset %}

        {% do return([ddl]) %}

    {% else %}

        {% do return([]) %}

    {% endif %}

{% endmacro %}

{% macro is_csv(file_format) %}

{# From https://docs.snowflake.net/manuals/sql-reference/sql/create-external-table.html:

Important: The external table does not inherit the file format, if any, in the
stage definition. You must explicitly specify any file format options for the
external table using the FILE_FORMAT parameter.

Note: FORMAT_NAME and TYPE are mutually exclusive; to avoid unintended behavior,
you should only specify one or the other when creating an external table.

#}

    {% set ff_ltrimmed = file_format|lower|replace(' ','') %}

    {% if 'type=' in ff_ltrimmed %}

        {% if 'type=csv' in ff_ltrimmed %}

            {{return(true)}}

        {% else %}

            {{return(false)}}

        {% endif %}

    {% else %}

        {% set ff_standardized = ff_ltrimmed
            | replace('(','') | replace(')','')
            | replace('format_name=','') %}
        {% set fqn = ff_standardized.split('.') %}

        {% if fqn | length == 3 %}
            {% set ff_database, ff_schema, ff_identifier = fqn[0], fqn[1], fqn[2] %}
        {% elif fqn | length == 2 %}
            {% set ff_database, ff_schema, ff_identifier = target.database, fqn[0], fqn[1] %}
        {% else %}
            {% set ff_database, ff_schema, ff_identifier = target.database, target.schema, fqn[0] %}
        {% endif %}

        {% call statement('get_file_format', fetch_result = True) %}
            show file formats in {{ff_database}}.{{ff_schema}}
        {% endcall %}

        {% set ffs = load_result('get_file_format').table %}

        {% for ff in ffs %}

            {% if ff['name']|lower == ff_identifier and ff['type']|lower == 'csv' %}

                {{return(true)}}

            {% endif %}

        {% endfor %}

        {{return(false)}}

    {% endif %}

{% endmacro %}
