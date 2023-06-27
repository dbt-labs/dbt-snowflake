{% materialization dynamic_table, adapter='snowflake' %}

    -- Try to create a valid dynamic table from the config before doing anything else
    {% set new_dynamic_table = adapter.Relation.from_runtime_config(config) %}

    -- We still need these because they tie into the existing process (e.g. RelationBase vs. RelationConfigBase)
    {% set existing_relation = load_cached_relation(this) %}
    {% set target_relation = this.incorporate(type=this.DynamicTable) %}

    {{ run_hooks(pre_hooks) }}

        {% set build_sql = dynamic_table_build_sql(new_dynamic_table, existing_relation) %}

        {% if build_sql == '' %}
            {{ dynamic_table_execute_no_op(new_dynamic_table) }}
        {% else %}
            {{ dynamic_table_execute_build_sql(build_sql, new_dynamic_table, post_hooks) }}
        {% endif %}

    {{ run_hooks(post_hooks) }}

    {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}


{% macro dynamic_table_build_sql(new_dynamic_table, existing_relation) %}

    {% set full_refresh_mode = should_full_refresh() %}

    -- determine the scenario we're in: create, full_refresh, alter
    {% if existing_relation is none %}
        {% set build_sql = create_dynamic_table_sql(new_dynamic_table) %}
    {% elif full_refresh_mode or not existing_relation.is_dynamic_table %}
        {% set build_sql = replace_dynamic_table_sql(new_dynamic_table, existing_relation) %}
    {% else %}
        {% set build_sql = alter_dynamic_table_with_on_configuration_change_option_sql(new_dynamic_table) %}
    {% endif %}

    {% do return(build_sql) %}

{% endmacro %}


{% macro alter_dynamic_table_with_on_configuration_change_option_sql(new_dynamic_table) %}

    {% set describe_relation_results = describe_dynamic_table(new_dynamic_table) %}
    {% set existing_dynamic_table = adapter.Relation.from_describe_relation_results(describe_relation_results, adapter.Relation.DynamicTable) %}
    {% set on_configuration_change = config.get('on_configuration_change') %}

    {% if new_dynamic_table == existing_dynamic_table %}
        {% set build_sql = refresh_dynamic_table_sql(new_dynamic_table) %}

    {% elif on_configuration_change == 'apply' %}
        {% set build_sql = alter_dynamic_table_sql(new_dynamic_table, existing_dynamic_table) %}
    {% elif on_configuration_change == 'continue' %}
        {% set build_sql = '' %}
        {{ exceptions.warn("Configuration changes were identified and `on_configuration_change` was set to `continue` for `" ~ new_dynamic_table.fully_qualified_path ~ "`") }}
    {% elif on_configuration_change == 'fail' %}
        {{ exceptions.raise_fail_fast_error("Configuration changes were identified and `on_configuration_change` was set to `fail` for `" ~ new_dynamic_table.fully_qualified_path ~ "`") }}

    {% else %}
        -- this only happens if the user provides a value other than `apply`, 'continue', 'fail'
        {{ exceptions.raise_compiler_error("Unexpected configuration scenario: `" ~ on_configuration_change ~ "`") }}

    {% endif %}

    {% do return(build_sql) %}

{% endmacro %}


{% macro dynamic_table_execute_no_op(new_dynamic_table) %}
    {% do store_raw_result(
        name="main",
        message="skip " ~ new_dynamic_table.fully_qualified_path,
        code="skip",
        rows_affected="-1"
    ) %}
{% endmacro %}


{% macro dynamic_table_execute_build_sql(build_sql, new_dynamic_table, post_hooks) %}

    {% set grant_config = config.get('grants') %}

    {% set original_query_tag = set_query_tag() %}

    {% call statement(name="main") %}
        {{ build_sql }}
    {% endcall %}

    {% do unset_query_tag(original_query_tag) %}

    {% set should_revoke = should_revoke(new_dynamic_table.fully_qualified_path, full_refresh_mode=True) %}
    {% do apply_grants(new_dynamic_table.fully_qualified_path, grant_config, should_revoke=should_revoke) %}

    {% do persist_docs(new_dynamic_table.fully_qualified_path, model) %}

{% endmacro %}
