{% macro snowflake__refresh_dynamic_table(relation) -%}
    {#-
    -- log because there is no generic refresh
    -#}
    {{- log('Applying REFRESH to: ' ~ relation) -}}

    alter dynamic table {{ relation }} refresh
{%- endmacro %}
