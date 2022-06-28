{% macro create_or_replace_materializedview() %}
  {%- set identifier = model['alias'] -%}

  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}

  {%- set exists_as_materializedview = (old_relation is not none and old_relation.is_materializedview) -%}

  {%- set target_relation = api.Relation.create(
      identifier=identifier, schema=schema, database=database,
      type='materialized view') -%}

  {{ run_hooks(pre_hooks) }}

  -- If there's an object with the same name it's not an MV, drop
  {%- if old_relation is not none and not exists_as_materialized_view -%}
    {{ handle_existing_table(should_full_refresh(), old_relation) }}
  {%- endif -%}

  {% if exists_as_materialized_view and not should_full_refresh() %}
  -- no action required
  {{ log("No action required on " ~ old_relation ~ " because it is a materialized view and full refresh is off") }}
  {%- else -%}
  -- build model
  {% call statement('main') -%}
    {{ get_create_materializedview_as_sql(target_relation, sql) }}
  {%- endcall %}

  {{ run_hooks(post_hooks) }}
  {%- endif -%}

  {{ return({'relations': [target_relation]}) }}

{% endmacro %}


{% macro get_create_materializedview_as_sql(relation, sql) -%}
  {{ adapter.dispatch('get_create_materializedview_as_sql', 'dbt')(relation, sql) }}
{%- endmacro %}

{% macro snowflake__get_create_materializedview_as_sql(relation, sql) -%}
    {%- set sql_header = config.get('sql_header', none) -%}
    {%- set cluster = config.get('cluster_by', none) -%}
  {{ sql_header if sql_header is not none }}
   create materialized view {{ relation }}
    {% if cluster is not none -%}
     cluster by ({{ cluster|join(',') }})
    {%- endif -%}
     as (
    {{ sql }}
  );
{% endmacro %}
