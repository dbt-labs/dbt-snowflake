{% materialization table, adapter='snowflake' %}

  {% set original_query_tag = set_query_tag() %}

  {%- set identifier = model['alias'] -%}

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

  {% if config.get('language', 'sql') == 'python' -%}}
    {%- set proc_name = api.Relation.create(identifier=identifier ~ "__dbt_sp",
                                                schema=schema,
                                                database=database) -%}
    {% set materialization_logic = py_materialize_as_table() %}
    {% set setup_stored_proc = py_create_stored_procedure(proc_name, materialization_logic, model, sql) %}

    {% do log("Creating stored procedure: " ~ proc_name, info=true) %}
    {% do run_query(setup_stored_proc) %}
    {% do log("Finished creating stored procedure: " ~ proc_name, info=true) %}

    --build model
    {% call statement('main') -%}
      CALL {{ proc_name }}();

    {%- endcall %}

    -- cleanup stuff
    {% do run_query("drop procedure if exists " ~ proc_name ~ "(string)") %}

  {%- else -%}
    --build model
    {% call statement('main') -%}
      {{ create_table_as(false, target_relation, sql) }}
    {%- endcall %}

  {%- endif %}

  {{ run_hooks(post_hooks) }}

  {% do persist_docs(target_relation, model) %}

  {% do unset_query_tag(original_query_tag) %}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}

{% macro py_materialize_as_table(config) %}
def materialize(session, df, target_relation):
    # we have to make sure pandas is imported
    import pandas
    if isinstance(df, pandas.core.frame.DataFrame):
        # session.write_pandas does not have overwrite function
        df = session.createDataFrame(df)
    df.write.mode("overwrite").save_as_table(str(target_relation))
{% endmacro %}

{% macro py_create_stored_procedure(proc_name, materialization_logic, model, user_supplied_logic) %}

{% set packages = ['snowflake-snowpark-python'] + config.get('packages', []) %}

CREATE OR REPLACE PROCEDURE {{ proc_name }} ()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8' -- TODO should this be configurable?
PACKAGES = ('{{ packages | join("', '") }}')
HANDLER = 'main'
AS
$$

{{ user_supplied_logic }}

{{ materialization_logic }}

def main(session):
    """
    TODOs:
      - what should this return? can we make a real RunResult?
    """
    dbt = dbtObj(session.table)
    df = model(dbt, session)
    materialize(session, df, dbt.this)
    return "OK"

$$;

{% endmacro %}
