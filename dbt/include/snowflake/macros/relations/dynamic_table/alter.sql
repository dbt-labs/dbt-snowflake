{% macro snowflake__get_alter_dynamic_table_as_sql(
    target_relation,
    configuration_changes,
    sql,
    existing_relation,
    backup_relation,
    intermediate_relation
) -%}
    {{- log('Applying ALTER to: ' ~ target_relation) -}}

    {% if configuration_changes.requires_full_refresh %}
        {{- snowflake__get_replace_dynamic_table_as_sql(target_relation, sql, existing_relation, backup_relation, intermediate_relation) -}}

    {% else %}

        {%- set target_lag = configuration_changes.target_lag -%}
        {%- if target_lag -%}{{- log('Applying UPDATE TARGET_LAG to: ' ~ existing_relation) -}}{%- endif -%}
        {%- set snowflake_warehouse = configuration_changes.snowflake_warehouse -%}
        {%- if snowflake_warehouse -%}{{- log('Applying UPDATE WAREHOUSE to: ' ~ existing_relation) -}}{%- endif -%}

        alter dynamic table {{ existing_relation }} set
            {% if target_lag %}target_lag = '{{ target_lag.context }}'{% endif %}
            {% if snowflake_warehouse %}warehouse = {{ snowflake_warehouse.context }}{% endif %}

    {%- endif -%}

{%- endmacro %}
