{% materialization seed, adapter='snowflake' %}
    {% set original_query_tag = set_query_tag() %}

    {% set relations = materialization_seed_default() %}

    {% do unset_query_tag(original_query_tag) %}

    {{ return(relations) }}
{% endmaterialization %}
