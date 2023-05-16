{% materialization dynamic_table, default %}
    {% set existing_relation = load_cached_relation(this) %}
    {% set target_relation = this.incorporate(type=this.Table) %}
    {% set intermediate_relation = make_intermediate_relation(target_relation) %}
    {% set backup_relation_type = target_relation.Table if existing_relation is none else existing_relation.type %}
    {% set backup_relation = make_backup_relation(target_relation, backup_relation_type) %}

    {{ _setup(backup_relation, intermediate_relation, pre_hooks) }}

        {% set build_sql = _get_build_sql(existing_relation, target_relation, backup_relation, intermediate_relation) %}

        {% if build_sql == '' %}
            {{ _execute_no_op(target_relation) }}
        {% else %}
            {{ _execute_build_sql(build_sql, existing_relation, target_relation, post_hooks) }}
        {% endif %}

    {{ _teardown(backup_relation, intermediate_relation, post_hooks) }}

    {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}


{% macro _setup(backup_relation, intermediate_relation, pre_hooks) %}

    -- backup_relation and intermediate_relation should not already exist in the database
    -- it's possible these exist because of a previous run that exited unexpectedly
    {% set preexisting_backup_relation = load_cached_relation(backup_relation) %}
    {% set preexisting_intermediate_relation = load_cached_relation(intermediate_relation) %}

    -- drop the temp relations if they exist already in the database
    {{ drop_relation_if_exists(preexisting_backup_relation) }}
    {{ drop_relation_if_exists(preexisting_intermediate_relation) }}

    {{ run_hooks(pre_hooks, inside_transaction=False) }}

{% endmacro %}


{% macro _teardown(backup_relation, intermediate_relation, post_hooks) %}

    -- drop the temp relations if they exist to leave the database clean for the next run
    {{ drop_relation_if_exists(backup_relation) }}
    {{ drop_relation_if_exists(intermediate_relation) }}

    {{ run_hooks(post_hooks, inside_transaction=False) }}

{% endmacro %}


{% macro _get_build_sql(existing_relation, target_relation, backup_relation, intermediate_relation) %}

    {% set full_refresh_mode = should_full_refresh() %}

    -- determine the scenario we're in: create, full_refresh, alter, refresh data
    {% if existing_relation is none %}
        {% set build_sql = snowflake__get_create_dynamic_table_as_sql(target_relation, sql) %}
    {% elif full_refresh_mode or not existing_relation.is_table %}
        {% set build_sql = snowflake__get_replace_dynamic_table_as_sql(target_relation, sql, existing_relation, backup_relation, intermediate_relation) %}
    {% else %}

        -- get config options
        {% set on_configuration_change = config.get('on_configuration_change') %}
        {% set configuration_changes = snowflake__get_dynamic_table_configuration_changes(existing_relation, config) %}

        {% if configuration_changes == {} %}
            {% set build_sql = snowflake__refresh_dynamic_table(target_relation) %}

        {% elif on_configuration_change == 'apply' %}
            {% set build_sql = snowflake__get_alter_dynamic_table_as_sql(target_relation, configuration_changes, sql, existing_relation, backup_relation, intermediate_relation) %}
        {% elif on_configuration_change == 'skip' %}
            {% set build_sql = '' %}
            {{ exceptions.warn("Configuration changes were identified and `on_configuration_change` was set to `skip` for `" ~ target_relation ~ "`") }}
        {% elif on_configuration_change == 'fail' %}
            {{ exceptions.raise_compiler_error("Configuration changes were identified and `on_configuration_change` was set to `fail` for `" ~ target_relation ~ "`") }}

        {% else %}
            -- this only happens if the user provides a value other than `apply`, 'skip', 'fail'
            {{ exceptions.raise_compiler_error("Unexpected configuration scenario") }}

        {% endif %}

    {% endif %}

    {% do return(build_sql) %}

{% endmacro %}


{% macro _execute_no_op(target_relation) %}
    {% do store_raw_result(
        name="main",
        message="skip " ~ target_relation,
        code="skip",
        rows_affected="-1"
    ) %}
{% endmacro %}


{% macro _execute_build_sql(build_sql, existing_relation, target_relation, post_hooks) %}

    -- `BEGIN` happens here:
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    {% set grant_config = config.get('grants') %}

    {% call statement(name="main") %}
        {{ build_sql }}
    {% endcall %}

    {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
    {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

    {% do persist_docs(target_relation, model) %}

    {{ run_hooks(post_hooks, inside_transaction=True) }}

    {{ adapter.commit() }}

{% endmacro %}
