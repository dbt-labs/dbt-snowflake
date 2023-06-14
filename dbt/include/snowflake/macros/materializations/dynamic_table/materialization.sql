{% materialization dynamic_table, adapter='snowflake' %}

    -- Try to create a valid dynamic table from the config before doing anything else
    {% set dynamic_table = relation.dynamic_table_from_runtime_config(config) %}

    {% set existing_relation = load_cached_relation(this) %}
    {% set target_relation = this.incorporate(type=this.DynamicTable) %}
    {% set intermediate_relation = make_intermediate_relation(target_relation) %}
    {% set backup_relation_type = target_relation.DynamicTable if existing_relation is none else existing_relation.type %}
    {% set backup_relation = make_backup_relation(target_relation, backup_relation_type) %}

    {{ dynamic_table_setup(backup_relation, intermediate_relation, pre_hooks) }}

        {% set build_sql = dynamic_table_build_sql(dynamic_table, existing_relation, backup_relation, intermediate_relation) %}

        {% if build_sql == '' %}
            {{ dynamic_table_execute_no_op(dynamic_table) }}
        {% else %}
            {{ dynamic_table_execute_build_sql(build_sql, dynamic_table, existing_relation, post_hooks) }}
        {% endif %}

    {{ dynamic_table_teardown(backup_relation, intermediate_relation, post_hooks) }}

    {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}


{% macro dynamic_table_setup(backup_relation, intermediate_relation, pre_hooks) %}

    -- backup_relation and intermediate_relation should not already exist in the database
    -- it's possible these exist because of a previous run that exited unexpectedly
    {% set preexisting_backup_relation = load_cached_relation(backup_relation) %}
    {% set preexisting_intermediate_relation = load_cached_relation(intermediate_relation) %}

    -- drop the temp relations if they exist already in the database
    {{ snowflake__get_drop_dynamic_table_sql(preexisting_backup_relation) }}
    {{ snowflake__get_drop_dynamic_table_sql(preexisting_intermediate_relation) }}

    {{ run_hooks(pre_hooks) }}

{% endmacro %}


{% macro dynamic_table_teardown(backup_relation, intermediate_relation, post_hooks) %}

    -- drop the temp relations if they exist to leave the database clean for the next run
    {{ snowflake__get_drop_dynamic_table_sql(backup_relation) }}
    {{ snowflake__get_drop_dynamic_table_sql(intermediate_relation) }}

    {{ run_hooks(post_hooks) }}

{% endmacro %}


{% macro dynamic_table_build_sql(dynamic_table, existing_relation, backup_relation, intermediate_relation) %}

    {% set full_refresh_mode = should_full_refresh() %}

    -- determine the scenario we're in: create, full_refresh, alter, refresh data
    {% if existing_relation is none %}
        {% set build_sql = snowflake__create_dynamic_table_sql(dynamic_table) %}
    {% elif full_refresh_mode or not existing_relation.is_dynamic_table %}
        {% set build_sql = snowflake__replace_dynamic_table_sql(dynamic_table, existing_relation, backup_relation, intermediate_relation) %}
    {% else %}

        -- get config options
        {% set on_configuration_change = config.get('on_configuration_change') %}
        {% set configuration_changes = snowflake__dynamic_table_configuration_changes(dynamic_table) %}

        {% if configuration_changes is none %}
            {% set build_sql = snowflake__refresh_dynamic_table(dynamic_table) %}

        {% elif on_configuration_change == 'apply' %}
            {% set build_sql = snowflake__alter_dynamic_table_sql(dynamic_table, configuration_changes, existing_relation, backup_relation, intermediate_relation) %}
        {% elif on_configuration_change == 'continue' %}
            {% set build_sql = '' %}
            {{ exceptions.warn("Configuration changes were identified and `on_configuration_change` was set to `continue` for `" ~ dynamic_table.name ~ "`") }}
        {% elif on_configuration_change == 'fail' %}
            {{ exceptions.raise_fail_fast_error("Configuration changes were identified and `on_configuration_change` was set to `fail` for `" ~ dynamic_table.name ~ "`") }}

        {% else %}
            -- this only happens if the user provides a value other than `apply`, 'continue', 'fail'
            {{ exceptions.raise_compiler_error("Unexpected configuration scenario: `" ~ on_configuration_change ~ "`") }}

        {% endif %}

    {% endif %}

    {% do return(build_sql) %}

{% endmacro %}


{% macro dynamic_table_execute_no_op(dynamic_table) %}
    {% do store_raw_result(
        name="main",
        message="skip " ~ dynamic_table.name,
        code="skip",
        rows_affected="-1"
    ) %}
{% endmacro %}


{% macro dynamic_table_execute_build_sql(build_sql, dynamic_table, existing_relation, post_hooks) %}

    {% set grant_config = config.get('grants') %}

    {% set original_query_tag = set_query_tag() %}

    {% call statement(name="main") %}
        {{ build_sql }}
    {% endcall %}

    {% do unset_query_tag(original_query_tag) %}

    {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
    {% do apply_grants(dynamic_table.name, grant_config, should_revoke=should_revoke) %}

    {% do persist_docs(dynamic_table.name, model) %}

{% endmacro %}
