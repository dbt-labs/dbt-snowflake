{% materialization snapshot, adapter='snowflake' %}
    {% set original_query_tag = set_query_tag() %}
    {% set  grant_config = config.get('grants') %}

    {% set relations = materialization_snapshot_default() %}
    {% do apply_grants(target_relation, grant_config, should_revoke=True) %}
    {% do unset_query_tag(original_query_tag) %}

    {{ return(relations) }}
{% endmaterialization %}
