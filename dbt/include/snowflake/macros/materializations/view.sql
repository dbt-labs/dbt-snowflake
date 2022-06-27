{% materialization view, adapter='snowflake' -%}

    {% set original_query_tag = set_query_tag() %}
    {% set to_return = create_or_replace_view() %}
    {% set  grant_config = config.get('grants') %}

    {% set target_relation = this.incorporate(type='view') %}

    {% do apply_grants(relation, grant_config, should_revoke=False) %}
    {% do persist_docs(target_relation, model, for_columns=false) %}

    {% do return(to_return) %}

    {% do unset_query_tag(original_query_tag) %}

{%- endmaterialization %}
