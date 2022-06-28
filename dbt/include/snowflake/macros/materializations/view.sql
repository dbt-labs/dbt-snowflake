{% materialization view, adapter='snowflake' -%}

    {% set original_query_tag = set_query_tag() %}
    {% set target_relation = this.incorporate(type='view') %}
    {%- set identifier = model['alias'] -%}
    {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}

    {{ run_hooks(pre_hooks) }}

    {% do persist_docs(target_relation, model, for_columns=false) %}

    -- if object exists already as MV, drop it before create_or_replace_view() macro
    {%- if old_relation is not none and old_relation.is_materializedview -%}
    {{ handle_existing_table(should_full_refresh(), old_relation) }}
    {%- endif -%}

    {% set to_return = create_or_replace_view() %}

    {% do return(to_return) %}

    {% do unset_query_tag(original_query_tag) %}

{%- endmaterialization %}
