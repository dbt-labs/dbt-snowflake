{% materialization table, adapter='snowflake' %}

  {% set original_query_tag = set_query_tag() %}

  {%- set identifier = model['alias'] -%}
  {%- set language = config.get('language') -%}

  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set target_relation = api.Relation.create(identifier=identifier,
                                                schema=schema,
                                                database=database, type='table') -%}

  {{ run_hooks(pre_hooks) }}

  {#-- Drop the relation if it was a view to "convert" it in a table. This may lead to
    -- downtime, but it should be a relatively infrequent occurrence  #}
  {% if old_relation is not none and not old_relation.is_table %}
    {{ log("Dropping relation " ~ old_relation ~ " because it is of type " ~ old_relation.type) }}
    {{ drop_relation_if_exists(old_relation) }}
  {% endif %}

  {% call statement('main', language=language) -%}
      {{ create_table_as(False, target_relation, compiled_code, language) }}
  {%- endcall %}

  {{ run_hooks(post_hooks) }}

  {% do persist_docs(target_relation, model) %}

  {% do unset_query_tag(original_query_tag) %}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}

{% macro py_complete_script(compiled_code, target_relation) %}
{{ compiled_code }}
def materialize(session, df, target_relation):
    # we have to make sure pandas is imported
    import pandas
    if isinstance(df, pandas.core.frame.DataFrame):
        # session.write_pandas does not have overwrite function
        df = session.createDataFrame(df)
    df.write.mode("overwrite").save_as_table("{{ target_relation }}")

def main(session):
    """
    TODOs:
      - what should this return? can we make a real RunResult?
    """
    dbt = dbtObj(session.table)
    df = model(dbt, session)
    materialize(session, df, dbt.this)
    return "OK"
{% endmacro %}
