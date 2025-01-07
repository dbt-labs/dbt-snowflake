{% materialization table, adapter='snowflake', supported_languages=['sql', 'python']%}

  {% set original_query_tag = set_query_tag() %}

  {%- set identifier = model['alias'] -%}
  {%- set language = model['language'] -%}

  {% set grant_config = config.get('grants') %}

  {%- set existing_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set target_relation = api.Relation.create(
	identifier=identifier,
	schema=schema,
	database=database,
	type='table',
	table_format=config.get('table_format', 'default')
    ) -%}

  {{ run_hooks(pre_hooks) }}

  {% if target_relation.needs_to_drop(existing_relation) %}
    {{ drop_relation_if_exists(existing_relation) }}
  {% endif %}

  {% call statement('main', language=language) -%}
      {{ create_table_as(False, target_relation, compiled_code, language) }}
  {%- endcall %}

  {{ run_hooks(post_hooks) }}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {% do unset_query_tag(original_query_tag) %}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}

{% macro py_write_table(compiled_code, target_relation, temporary=False, table_type=none) %}
{#- The following logic is only for backwards-compatiblity with deprecated `temporary` parameter -#}
{% if table_type is not none %}
    {#- Just use the table_type as-is -#}
{% elif temporary -%}
    {#- Case 1 when the deprecated `temporary` parameter is used without the replacement `table_type` parameter -#}
    {%- set table_type = "temporary" -%}
{% else %}
    {#- Case 2 when the deprecated `temporary` parameter is used without the replacement `table_type` parameter -#}
    {#- Snowflake treats "" as meaning "permanent" -#}
    {%- set table_type = "" -%}
{%- endif %}
{{ compiled_code }}
def materialize(session, df, target_relation):
    # make sure pandas exists
    import importlib.util
    package_name = 'pandas'
    if importlib.util.find_spec(package_name):
        import pandas
        if isinstance(df, pandas.core.frame.DataFrame):
          session.use_database(target_relation.database)
          session.use_schema(target_relation.schema)
          # session.write_pandas does not have overwrite function
          df = session.createDataFrame(df)
    {% set target_relation_name = resolve_model_name(target_relation) %}
    df.write.mode("overwrite").save_as_table('{{ target_relation_name }}', table_type='{{table_type}}')

def main(session):
    dbt = dbtObj(session.table)
    df = model(dbt, session)
    materialize(session, df, dbt.this)
    return "OK"
{% endmacro %}
