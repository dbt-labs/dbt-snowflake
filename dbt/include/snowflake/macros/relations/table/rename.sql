{%- macro snowflake__get_rename_table_sql(relation, new_name) -%}
    alter table {{ relation }} rename to {{ relation.incorporate(identifier=new_name) }}
{%- endmacro -%}
