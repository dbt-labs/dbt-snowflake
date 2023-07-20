{% macro get_current_query_tag() -%}
  {{ return(run_query("show parameters like 'query_tag' in session").rows[0]['value']) }}
{% endmacro %}


{% macro set_query_tag() -%}
    {{ return(adapter.dispatch('set_query_tag', 'dbt')()) }}
{% endmacro %}


{% macro snowflake__set_query_tag() -%}
  {% set new_query_tag = config.get('query_tag') %}
  {% if new_query_tag %}
    {% set original_query_tag = get_current_query_tag() %}
    {{ log("Setting query_tag to '" ~ new_query_tag ~ "'. Will reset to '" ~ original_query_tag ~ "' after materialization.") }}
    {% do run_query("alter session set query_tag = '{}'".format(new_query_tag)) %}
    {{ return(original_query_tag)}}
  {% endif %}
  {{ return(none)}}
{% endmacro %}


{% macro unset_query_tag(original_query_tag) -%}
    {{ return(adapter.dispatch('unset_query_tag', 'dbt')(original_query_tag)) }}
{% endmacro %}


{% macro snowflake__unset_query_tag(original_query_tag) -%}
  {% set new_query_tag = config.get('query_tag') %}
  {% if new_query_tag %}
    {% if original_query_tag %}
      {{ log("Resetting query_tag to '" ~ original_query_tag ~ "'.") }}
      {% do run_query("alter session set query_tag = '{}'".format(original_query_tag)) %}
    {% else %}
      {{ log("No original query_tag, unsetting parameter.") }}
      {% do run_query("alter session unset query_tag") %}
    {% endif %}
  {% endif %}
{% endmacro %}
