{%- macro snowflake__get_rename_view_sql(relation, new_name) -%}
    alter view {{ relation }} rename to {{ new_name }}
{%- endmacro -%}
