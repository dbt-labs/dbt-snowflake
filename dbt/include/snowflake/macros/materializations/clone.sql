{% macro snowflake__can_clone_tables() %}
    {{ return(True) }}
{% endmacro %}

{% macro snowflake__get_clone_table_sql(this_relation, state_relation) %}
    create or replace
      {{ "transient" if config.get("transient", true) }}
      table {{ this_relation }}
      clone {{ state_relation }}
      {{ "copy grants" if config.get("copy_grants", false) }}
{% endmacro %}
