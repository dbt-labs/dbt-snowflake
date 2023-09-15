{% macro snowflake__create_table_as(temporary, relation, compiled_code, language='sql') -%}
  {%- if language == 'sql' -%}
    {%- set transient = config.get('transient', default=true) -%}
    {%- set cluster_by_keys = config.get('cluster_by', default=none) -%}
    {%- set enable_automatic_clustering = config.get('automatic_clustering', default=false) -%}
    {%- set copy_grants = config.get('copy_grants', default=false) -%}

    {%- if cluster_by_keys is not none and cluster_by_keys is string -%}
      {%- set cluster_by_keys = [cluster_by_keys] -%}
    {%- endif -%}
    {%- if cluster_by_keys is not none -%}
      {%- set cluster_by_string = cluster_by_keys|join(", ")-%}
    {% else %}
      {%- set cluster_by_string = none -%}
    {%- endif -%}
    {%- set sql_header = config.get('sql_header', none) -%}

    {{ sql_header if sql_header is not none }}

        create or replace {% if temporary -%}
          temporary
        {%- elif transient -%}
          transient
        {%- endif %} table {{ relation }}
        {%- set contract_config = config.get('contract') -%}
        {%- if contract_config.enforced -%}
          {{ get_assert_columns_equivalent(sql) }}
          {{ get_table_columns_and_constraints() }}
          {% set compiled_code = get_select_subquery(compiled_code) %}
        {% endif %}
        {% if copy_grants and not temporary -%} copy grants {%- endif %} as
        (
          {%- if cluster_by_string is not none -%}
            select * from (
              {{ compiled_code }}
              ) order by ({{ cluster_by_string }})
          {%- else -%}
            {{ compiled_code }}
          {%- endif %}
        );
      {% if cluster_by_string is not none and not temporary -%}
        alter table {{relation}} cluster by ({{cluster_by_string}});
      {%- endif -%}
      {% if enable_automatic_clustering and cluster_by_string is not none and not temporary  -%}
        alter table {{relation}} resume recluster;
      {%- endif -%}

  {%- elif language == 'python' -%}
    {{ py_write_table(compiled_code=compiled_code, target_relation=relation, temporary=temporary) }}
  {%- else -%}
      {% do exceptions.raise_compiler_error("snowflake__create_table_as macro didn't get supported language, it got %s" % language) %}
  {%- endif -%}

{% endmacro %}

{% macro get_column_comment_sql(column_name, column_dict) -%}
  {% if (column_name|upper in column_dict) -%}
    {% set matched_column = column_name|upper -%}
  {% elif (column_name|lower in column_dict) -%}
    {% set matched_column = column_name|lower -%}
  {% elif (column_name in column_dict) -%}
    {% set matched_column = column_name -%}
  {% else -%}
    {% set matched_column = None -%}
  {% endif -%}
  {% if matched_column -%}
    {{ adapter.quote(column_name) }} COMMENT $${{ column_dict[matched_column]['description'] | replace('$', '[$]') }}$$
  {%- else -%}
    {{ adapter.quote(column_name) }} COMMENT $$$$
  {%- endif -%}
{% endmacro %}

{% macro get_persist_docs_column_list(model_columns, query_columns) %}
(
  {% for column_name in query_columns %}
    {{ get_column_comment_sql(column_name, model_columns) }}
    {{- ", " if not loop.last else "" }}
  {% endfor %}
)
{% endmacro %}

{% macro snowflake__create_view_as_with_temp_flag(relation, sql, is_temporary=False) -%}
  {%- set secure = config.get('secure', default=false) -%}
  {%- set copy_grants = config.get('copy_grants', default=false) -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}
  create or replace {% if secure -%}
    secure
  {%- endif %} {% if is_temporary -%}
    temporary
  {%- endif %} view {{ relation }}
  {% if config.persist_column_docs() -%}
    {% set model_columns = model.columns %}
    {% set query_columns = get_columns_in_query(sql) %}
    {{ get_persist_docs_column_list(model_columns, query_columns) }}

  {%- endif %}
  {%- set contract_config = config.get('contract') -%}
  {%- if contract_config.enforced -%}
    {{ get_assert_columns_equivalent(sql) }}
  {%- endif %}
  {% if copy_grants -%} copy grants {%- endif %} as (
    {{ sql }}
  );
{% endmacro %}

{% macro snowflake__create_view_as(relation, sql) -%}
  {{ snowflake__create_view_as_with_temp_flag(relation, sql) }}
{% endmacro %}

{% macro snowflake__get_columns_in_relation(relation) -%}
  {%- set sql -%}
    describe table {{ relation }}
  {%- endset -%}
  {%- set result = run_query(sql) -%}

  {% set maximum = 10000 %}
  {% if (result | length) >= maximum %}
    {% set msg %}
      Too many columns in relation {{ relation }}! dbt can only get
      information about relations with fewer than {{ maximum }} columns.
    {% endset %}
    {% do exceptions.raise_compiler_error(msg) %}
  {% endif %}

  {% set columns = [] %}
  {% for row in result %}
    {% do columns.append(api.Column.from_description(row['name'], row['type'])) %}
  {% endfor %}
  {% do return(columns) %}
{% endmacro %}

{% macro snowflake__list_schemas(database) -%}
  {# 10k limit from here: https://docs.snowflake.net/manuals/sql-reference/sql/show-schemas.html#usage-notes #}
  {% set maximum = 10000 %}
  {% set sql -%}
    show terse schemas in database {{ database }}
    limit {{ maximum }}
  {%- endset %}
  {% set result = run_query(sql) %}
  {% if (result | length) >= maximum %}
    {% set msg %}
      Too many schemas in database {{ database }}! dbt can only get
      information about databases with fewer than {{ maximum }} schemas.
    {% endset %}
    {% do exceptions.raise_compiler_error(msg) %}
  {% endif %}
  {{ return(result) }}
{% endmacro %}


{% macro snowflake__get_paginated_relations_array(max_iter, max_results_per_iter, max_total_results, schema_relation, watermark) %}

  {% set paginated_relations = [] %}

  {% for _ in range(0, max_iter) %}

      {%- set paginated_sql -%}
         show terse objects in {{ schema_relation }} limit {{ max_results_per_iter }} from '{{ watermark.table_name }}'
      {%- endset -%}

      {%- set paginated_result = run_query(paginated_sql) %}
      {%- set paginated_n = (paginated_result | length) -%}

      {#
        terminating condition: if there are 0 records in the result we reached
        the end exactly on the previous iteration
      #}
      {%- if paginated_n == 0 -%}
        {%- break -%}
      {%- endif -%}

      {#
        terminating condition: At some point the user needs to be reasonable with how
        many objects are contained in their schemas. Since there was already
        one iteration before attempting pagination, loop.index == max_iter means
        the limit has been surpassed.
      #}

      {%- if loop.index == max_iter -%}
        {%- set msg -%}
           dbt will list a maximum of {{ max_total_results }} objects in schema {{ schema_relation }}.
           Your schema exceeds this limit. Please contact support@getdbt.com for troubleshooting tips,
           or review and reduce the number of objects contained.
        {%- endset -%}

        {% do exceptions.raise_compiler_error(msg) %}
      {%- endif -%}

      {%- do paginated_relations.append(paginated_result) -%}
      {% set watermark.table_name = paginated_result.columns[1].values()[-1] %}

      {#
        terminating condition: paginated_n < max_results_per_iter means we reached the end
      #}
      {%- if paginated_n < max_results_per_iter -%}
         {%- break -%}
      {%- endif -%}
    {%- endfor -%}

  {{ return(paginated_relations) }}

{% endmacro %}

{% macro snowflake__list_relations_without_caching(schema_relation, max_iter=10, max_results_per_iter=10000) %}

  {%- set max_total_results = max_results_per_iter * max_iter -%}

  {%- set sql -%}
    show terse objects in {{ schema_relation }} limit {{ max_results_per_iter }}
  {%- endset -%}

  {%- set result = run_query(sql) -%}

  {%- set n = (result | length) -%}
  {%- set watermark = namespace(table_name=result.columns[1].values()[-1]) -%}
  {%- set paginated = namespace(result=[]) -%}

  {% if n >= max_results_per_iter %}

    {% set paginated.result = snowflake__get_paginated_relations_array(
         max_iter,
         max_results_per_iter,
         max_total_results,
         schema_relation,
         watermark
       )
    %}

  {% endif %}

  {%- set all_results_array = [result] + paginated.result -%}
  {%- set result = result.merge(all_results_array) -%}
  {%- do return(result) -%}

{% endmacro %}


{% macro snowflake__check_schema_exists(information_schema, schema) -%}
  {% call statement('check_schema_exists', fetch_result=True) -%}
        select count(*)
        from {{ information_schema }}.schemata
        where upper(schema_name) = upper('{{ schema }}')
            and upper(catalog_name) = upper('{{ information_schema.database }}')
  {%- endcall %}
  {{ return(load_result('check_schema_exists').table) }}
{%- endmacro %}


{% macro snowflake__rename_relation(from_relation, to_relation) -%}
  {% call statement('rename_relation') -%}
    alter table {{ from_relation }} rename to {{ to_relation }}
  {%- endcall %}
{% endmacro %}


{% macro snowflake__alter_column_type(relation, column_name, new_column_type) -%}
  {% call statement('alter_column_type') %}
    alter table {{ relation }} alter {{ adapter.quote(column_name) }} set data type {{ new_column_type }};
  {% endcall %}
{% endmacro %}

{% macro snowflake__alter_relation_comment(relation, relation_comment) -%}
    {%- if relation.is_dynamic_table -%}
        {%- set relation_type = 'dynamic table' -%}
    {%- else -%}
        {%- set relation_type = relation.type -%}
    {%- endif -%}
    comment on {{ relation_type }} {{ relation }} IS $${{ relation_comment | replace('$', '[$]') }}$$;
{% endmacro %}


{% macro snowflake__alter_column_comment(relation, column_dict) -%}
    {% set existing_columns = adapter.get_columns_in_relation(relation) | map(attribute="name") | list %}
    {% if relation.is_dynamic_table -%}
        {% set relation_type = "dynamic table" %}
    {% else -%}
        {% set relation_type = relation.type %}
    {% endif %}
    alter {{ relation_type }} {{ relation }} alter
    {% for column_name in existing_columns if (column_name in existing_columns) or (column_name|lower in existing_columns) %}
        {{ get_column_comment_sql(column_name, column_dict) }} {{- ',' if not loop.last else ';' }}
    {% endfor %}
{% endmacro %}


{% macro get_current_query_tag() -%}
  {{ return(run_query("show parameters like 'query_tag' in session").rows[0]['value']) }}
{% endmacro %}


{% macro set_query_tag() -%}
    {{ return(adapter.dispatch('set_query_tag', 'dbt')()) }}
{% endmacro %}


{% macro snowflake__set_query_tag() -%}
  {% set new_query_tag = config.get('query_tag') %}
  {% if new_query_tag %}
    {% set original_query_tag = get_current_query_tag() %}
    {{ log("Setting query_tag to '" ~ new_query_tag ~ "'. Will reset to '" ~ original_query_tag ~ "' after materialization.") }}
    {% do run_query("alter session set query_tag = '{}'".format(new_query_tag)) %}
    {{ return(original_query_tag)}}
  {% endif %}
  {{ return(none)}}
{% endmacro %}


{% macro unset_query_tag(original_query_tag) -%}
    {{ return(adapter.dispatch('unset_query_tag', 'dbt')(original_query_tag)) }}
{% endmacro %}


{% macro snowflake__unset_query_tag(original_query_tag) -%}
  {% set new_query_tag = config.get('query_tag') %}
  {% if new_query_tag %}
    {% if original_query_tag %}
      {{ log("Resetting query_tag to '" ~ original_query_tag ~ "'.") }}
      {% do run_query("alter session set query_tag = '{}'".format(original_query_tag)) %}
    {% else %}
      {{ log("No original query_tag, unsetting parameter.") }}
      {% do run_query("alter session unset query_tag") %}
    {% endif %}
  {% endif %}
{% endmacro %}


{% macro snowflake__alter_relation_add_remove_columns(relation, add_columns, remove_columns) %}

    {% if relation.is_dynamic_table -%}
        {% set relation_type = "dynamic table" %}
    {% else -%}
        {% set relation_type = relation.type %}
    {% endif %}

    {% if add_columns %}

    {% set sql -%}
       alter {{ relation_type }} {{ relation }} add column
          {% for column in add_columns %}
            {{ column.name }} {{ column.data_type }}{{ ',' if not loop.last }}
          {% endfor %}
    {%- endset -%}

    {% do run_query(sql) %}

    {% endif %}

    {% if remove_columns %}

    {% set sql -%}
        alter {{ relation_type }} {{ relation }} drop column
            {% for column in remove_columns %}
                {{ column.name }}{{ ',' if not loop.last }}
            {% endfor %}
    {%- endset -%}

    {% do run_query(sql) %}

    {% endif %}

{% endmacro %}



{% macro snowflake_dml_explicit_transaction(dml) %}
  {#
    Use this macro to wrap all INSERT, MERGE, UPDATE, DELETE, and TRUNCATE
    statements before passing them into run_query(), or calling in the 'main' statement
    of a materialization
  #}
  {% set dml_transaction -%}
    begin;
    {{ dml }};
    commit;
  {%- endset %}

  {% do return(dml_transaction) %}

{% endmacro %}


{% macro snowflake__truncate_relation(relation) -%}
  {% set truncate_dml %}
    truncate table {{ relation }}
  {% endset %}
  {% call statement('truncate_relation') -%}
    {{ snowflake_dml_explicit_transaction(truncate_dml) }}
  {%- endcall %}
{% endmacro %}


{% macro snowflake__drop_relation(relation) -%}
    {%- if relation.is_dynamic_table -%}
        {% call statement('drop_relation', auto_begin=False) -%}
            drop dynamic table if exists {{ relation }}
        {%- endcall %}
    {%- else -%}
        {{- default__drop_relation(relation) -}}
    {%- endif -%}
{% endmacro %}
