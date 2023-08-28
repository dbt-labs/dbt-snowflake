{% macro snowflake__drop_relation(relation) -%}
    {%- if relation.is_dynamic_table -%}
        {% call statement('drop_relation', auto_begin=False) -%}
            drop dynamic table if exists {{ relation }}
        {%- endcall %}
    {%- else -%}
        {{- default__drop_relation(relation) -}}
    {%- endif -%}
{% endmacro %}
