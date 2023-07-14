{% materialization dynamic_table, adapter='snowflake' %}

    -- Try to create a valid dynamic table from the config before doing anything else
    {%- set materialization = adapter.make_materialization_from_node(config.model) -%}

    {% set build_sql = dynamic_table_build_sql(materialization) %}

    {{ run_hooks(pre_hooks) }}

    {% if build_sql == '' %}
        {{ execute_no_op(materialization) }}
    {% else %}
        {{ dynamic_table_execute_build_sql(materialization, build_sql) }}
    {% endif %}

    {{ run_hooks(post_hooks) }}

    {%- set target_relation = adapter.base_relation_from_relation_model(materialization.target_relation) -%}
    {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}


{%- macro dynamic_table_build_sql(materialization) -%}

    {%- if materialization.build_strategy == 'no_op' -%}
        {%- set build_sql = '' -%}

    {%- elif materialization.build_strategy == 'create' -%}
        {%- set build_sql = create_template(materialization.target_relation) -%}

    {%- elif materialization.build_strategy == 'replace' -%}
        {%- set build_sql = replace_template(
            materialization.existing_relation_ref, materialization.target_relation
        ) -%}

    {%- elif materialization.build_strategy == 'alter' -%}

        {% set describe_relation_results = describe_template(materialization.existing_relation_ref ) %}
        {% set existing_relation = materialization.existing_relation(describe_relation_results) %}

        {%- if materialization.on_configuration_change == 'apply' -%}
            {%- set build_sql = alter_template(existing_relation, materialization.target_relation) -%}

        {%- elif materialization.on_configuration_change == 'continue' -%}
            {%- set build_sql = '' -%}
            {{- exceptions.warn(
                "Configuration changes were identified and `on_configuration_change` "
                "was set to `continue` for `" ~ materialization.target_relation ~ "`"
            ) -}}

        {%- elif materialization.on_configuration_change == 'fail' -%}
            {%- set build_sql = '' -%}
            {{- exceptions.raise_fail_fast_error(
                "Configuration changes were identified and `on_configuration_change` "
                "was set to `fail` for `" ~ materialization.target_relation ~ "`"
            ) -}}

        {%- endif -%}

    {%- else -%}

        {{- exceptions.raise_compiler_error("This build strategy is not supported for dynamic tables: " ~ materialization.build_strategy) -}}

    {%- endif -%}

    {%- do return(build_sql) -%}

{% endmacro %}


{% macro dynamic_table_execute_build_sql(materialization, build_sql) %}

    {% set original_query_tag = set_query_tag() %}

    {% call statement(name="main") %}
        {{ build_sql }}
    {% endcall %}

    {% do unset_query_tag(original_query_tag) %}

    {% do apply_grants(materialization, materialization.grant_config, materialization.should_revoke_grants) %}

    {% do persist_docs(materialization.target_relation.fully_qualified_path, model) %}

{% endmacro %}
