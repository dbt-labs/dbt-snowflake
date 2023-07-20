{% macro dbt_snowflake_get_tmp_relation_type(strategy, unique_key, language) %}
{%- set tmp_relation_type = config.get('tmp_relation_type') -%}
  /* {#
       High-level principles:
       If we are running multiple statements (DELETE + INSERT),
       and we want to guarantee identical inputs to both statements,
       then we must first save the model query results as a temporary table
       (which presumably comes with a performance cost).
       If we are running a single statement (MERGE or INSERT alone),
       we _may_ save the model query definition as a view instead,
       for (presumably) faster overall incremental processing.

       Low-level specifics:
       If an invalid option is specified, then we will raise an
       excpetion with corresponding message.

       Languages other than SQL (like Python) will use a temporary table.
       With the default strategy of merge, the user may choose between a temporary
       table and view (defaulting to view).

       The append strategy can use a view because it will run a single INSERT statement.

       When unique_key is none, the delete+insert strategy can use a view beacuse a
       single INSERT statement is run with no DELETES as part of the statement.
       Otherwise, play it safe by using a temporary table.
  #} */

  {% if language == "python" and tmp_relation_type is not none %}
    {% do exceptions.raise_compiler_error(
      "Python models currently only support 'table' for tmp_relation_type but "
       ~ tmp_relation_type ~ " was specified."
    ) %}
  {% endif %}

  {% if strategy == "delete+insert" and tmp_relation_type is not none and tmp_relation_type != "table" and unique_key is not none %}
    {% do exceptions.raise_compiler_error(
      "In order to maintain consistent results when `unique_key` is not none,
      the `delete+insert` strategy only supports `table` for `tmp_relation_type` but "
      ~ tmp_relation_type ~ " was specified."
      )
  %}
  {% endif %}

  {% if language != "sql" %}
    {{ return("table") }}
  {% elif tmp_relation_type == "table" %}
    {{ return("table") }}
  {% elif tmp_relation_type == "view" %}
    {{ return("view") }}
  {% elif strategy in ("default", "merge", "append") %}
    {{ return("view") }}
  {% elif strategy == "delete+insert" and unique_key is none %}
    {{ return("view") }}
  {% else %}
    {{ return("table") }}
  {% endif %}
{% endmacro %}


{% macro snowflake__get_incremental_default_sql(arg_dict) %}
  {{ return(get_incremental_merge_sql(arg_dict)) }}
{% endmacro %}
