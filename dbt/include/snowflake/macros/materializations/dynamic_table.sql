{% materialization dynamic_table, adapter='snowflake' %}

    {% set original_query_tag = set_query_tag() %}

    {% set existing_relation = load_cached_relation(this) %}
    {% set target_relation = this.incorporate(type=this.DynamicTable) %}
    {% set intermediate_relation = make_intermediate_relation(target_relation) %}
    {% set backup_relation_type = target_relation.DynamicTable if existing_relation is none else existing_relation.type %}
    {% set backup_relation = make_backup_relation(target_relation, backup_relation_type) %}

    {{ dynamic_table_setup(backup_relation, intermediate_relation, pre_hooks) }}

        {% set build_sql = dynamic_table_get_build_sql(existing_relation, target_relation, backup_relation, intermediate_relation) %}

        {% if build_sql == '' %}
            {{ dynamic_table_execute_no_op(target_relation) }}
        {% else %}
            {{ dynamic_table_execute_build_sql(build_sql, existing_relation, target_relation, post_hooks) }}
        {% endif %}

    {{ dynamic_table_teardown(backup_relation, intermediate_relation, post_hooks) }}

    {% do unset_query_tag(original_query_tag) %}

    {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
