{% macro snowflake__create_table_as(temporary, relation, compiled_code, language='sql') -%}
  {%- set transient = config.get('transient', default=true) -%}
  {%- set iceberg = config.get('object_format', default='') == 'iceberg' -%}

  {%- if transient and iceberg -%}
    {% do exceptions.raise_compiler_error("Iceberg format relations cannot be transient. Please remove either the transient or iceberg parameters from %s" % this) %}
  {%- endif %}

  {%- if transient and iceberg -%}
    {% do exceptions.raise_compiler_error("Iceberg format relations cannot be transient. Please remove either the transient or iceberg parameters from %s" % this) %}
  {%- endif %}

  {# Configure for extended Object Format #}
  {% if iceberg -%}
    {%- set object_format = 'iceberg' -%}
  {%- else -%}
    {%- set object_format = '' -%}
  {%- endif -%}

  {# Configure for plain Table materialization #}
  {% if temporary -%}
    {%- set table_type = "temporary" -%}
  {%- elif transient -%}
    {%- set table_type = "transient" -%}
  {%- else -%}
    {%- set table_type = "" -%}
  {%- endif %}

  {%- set materialization_prefix = object_format or table_type -%}
  {%- set alter_statement_format_prefix = object_format -%}

  {# Generate DDL/DML #}
  {%- if language == 'sql' -%}
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

        create or replace {{ materialization_prefix }} table {{ relation }}
        {%- if iceberg %}
        external_volume = {{ config.get('external_volume') }}
        catalog = 'snowflake'
        base_location = {{ config.get('base_location') }}
        {%- endif -%}

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
        alter {{ alter_statement_format_prefix }} table {{relation}} cluster by ({{cluster_by_string}});
      {%- endif -%}
      {% if enable_automatic_clustering and cluster_by_string is not none and not temporary  -%}
        alter {{ alter_statement_format_prefix }} table {{relation}} resume recluster;
      {%- endif -%}

  {%- elif language == 'python' -%}
    {%- if iceberg -%}
      {% do exceptions.raise_compiler_error('Iceberg is incompatible with Python models. Please use a SQL model for the iceberg format.') %}
    {%- endif %}
    {{ py_write_table(compiled_code=compiled_code, target_relation=relation, table_type=table_type) }}
  {%- else -%}
      {% do exceptions.raise_compiler_error("snowflake__create_table_as macro didn't get supported language, it got %s" % language) %}
  {%- endif -%}

{% endmacro %}
