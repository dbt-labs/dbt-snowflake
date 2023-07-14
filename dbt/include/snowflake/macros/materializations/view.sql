{% materialization view, adapter='snowflake' -%}

    {% set original_query_tag = set_query_tag() %}

    {% set existing_relation = load_cached_relation(this) %}
    {% if existing_relation.type == 'dynamic_table' %}
        {{ snowflake__drop_relation(existing_relation) }}
    {% endif %}

    {% set target_relation = this.incorporate(type='view') %}

    {% set to_return = create_or_replace_view() %}

    {% do persist_docs(target_relation, model, for_columns=false) %}

    {% do unset_query_tag(original_query_tag) %}

    {% do return(to_return) %}

{%- endmaterialization %}
