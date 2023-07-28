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


{% macro dynamic_table_get_build_sql(existing_relation, target_relation, backup_relation, intermediate_relation) %}

    {% set full_refresh_mode = should_full_refresh() %}

    -- determine the scenario we're in: create, full_refresh, alter, refresh data
    {% if existing_relation is none %}
        {% set build_sql = snowflake__get_create_dynamic_table_as_sql(target_relation, sql) %}
    {% elif full_refresh_mode or not existing_relation.is_dynamic_table %}
        {% set build_sql = snowflake__get_replace_dynamic_table_as_sql(target_relation, sql, existing_relation, backup_relation, intermediate_relation) %}
    {% else %}

        -- get config options
        {% set on_configuration_change = config.get('on_configuration_change') %}
        {% set configuration_changes = snowflake__get_dynamic_table_configuration_changes(existing_relation, config) %}

        {% if configuration_changes is none %}
            {% set build_sql = '' %}
            {{ exceptions.warn("No configuration changes were identified on: `" ~ target_relation ~ "`. Continuing.") }}

        {% elif on_configuration_change == 'apply' %}
            {% set build_sql = snowflake__get_alter_dynamic_table_as_sql(target_relation, configuration_changes, sql, existing_relation, backup_relation, intermediate_relation) %}
        {% elif on_configuration_change == 'continue' %}
            {% set build_sql = '' %}
            {{ exceptions.warn("Configuration changes were identified and `on_configuration_change` was set to `continue` for `" ~ target_relation ~ "`") }}
        {% elif on_configuration_change == 'fail' %}
            {{ exceptions.raise_fail_fast_error("Configuration changes were identified and `on_configuration_change` was set to `fail` for `" ~ target_relation ~ "`") }}

        {% else %}
            -- this only happens if the user provides a value other than `apply`, 'continue', 'fail'
            {{ exceptions.raise_compiler_error("Unexpected configuration scenario: `" ~ on_configuration_change ~ "`") }}

        {% endif %}

    {% endif %}

    {% do return(build_sql) %}

{% endmacro %}


{% macro dynamic_table_execute_no_op(target_relation) %}
    {% do store_raw_result(
        name="main",
        message="skip " ~ target_relation,
        code="skip",
        rows_affected="-1"
    ) %}
{% endmacro %}


{% macro dynamic_table_execute_build_sql(build_sql, existing_relation, target_relation, post_hooks) %}

    {% set grant_config = config.get('grants') %}

    {% call statement(name="main") %}
        {{ build_sql }}
    {% endcall %}

    {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
    {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

    {% do persist_docs(target_relation, model) %}

{% endmacro %}
