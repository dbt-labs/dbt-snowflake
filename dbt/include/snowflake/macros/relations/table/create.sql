{% macro snowflake__create_table_as(temporary, relation, compiled_code, language='sql') -%}
  {%- set materialization_prefix = get_create_ddl_prefix(temporary) -%}
  {%- set alter_prefix = get_alter_ddl_prefix() -%}

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
        {%- if _is_iceberg_relation() %}
          {#
            Valid DDL in CTAS statements. Plain create statements have a different order.
            https://docs.snowflake.com/en/sql-reference/sql/create-iceberg-table
          #}
          {{ render_iceberg_ddl(relation) }}
        {% else %}
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
        alter {{ alter_prefix }} table {{relation}} cluster by ({{cluster_by_string}});
      {%- endif -%}
      {% if enable_automatic_clustering and cluster_by_string is not none and not temporary %}
        alter {{ alter_prefix }} table {{relation}} resume recluster;
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


{#
 # Helper Macros
 #}

{% macro get_create_ddl_prefix(temporary) %}
  {#
    This macro generates the appropriate DDL prefix for creating a table in Snowflake,
    considering the mutually exclusive nature of certain table types:

    - ICEBERG: A specific storage format that requires a distinct DDL layout.
    - TEMPORARY: Indicates a table that exists only for the duration of the session.
    - TRANSIENT: A type of table that is similar to a permanent table but without fail-safe.

    Note: If ICEBERG is specified, transient=true throws a warning because ICEBERG
      does not support transient tables.
  #}

  {%- set is_iceberg   = _is_iceberg_relation() -%}
  {%- set is_temporary = temporary -%}

  {%- if is_iceberg -%}
      {# -- Check if user supplied a transient model config of True. #}
      {%- if config.get('transient') == True -%}
        {{ exceptions.warn("Iceberg format relations cannot be transient. Please remove either the transient or iceberg parameters from %s. If left unmodified, dbt will ignore 'transient'." % this) }}
      {%- endif %}

      {# -- Check if runtime is trying to create a Temporary Iceberg table. #}
      {%- if is_temporary -%}
	{{ exceptions.raise_compiler_error("Iceberg format relations cannot be temporary. Temporary is being inserted into an Iceberg format table while materializing %s." % this) }}
      {%- endif %}

    {{ return('iceberg') }}

  {%- elif is_temporary -%}
    {{ return('temporary') }}

  {# -- Always supply transient on table create DDL unless user specifically sets transient to false or None. #}
  {%- elif config.get('transient') is not defined or config.get('transient') == True -%}
    {{ return('transient') }}

  {%- else -%}
    {{ return('') }}
  {%- endif -%}
{% endmacro %}


{% macro get_alter_ddl_prefix() %}
  {# All ALTER statements on Iceberg tables require an ICEBERG prefix #}
  {%- if _get_relation_object_format() == 'iceberg' -%}
    {{ return('iceberg') }}
  {%- else -%}
    {{ return('') }}
  {%- endif -%}
{% endmacro %}


{% macro _get_relation_object_format() %}
  {{ return(config.get('object_format', default='')) }}
{% endmacro %}


{% macro _is_iceberg_relation() %}
  {{ return(_get_relation_object_format() == 'iceberg') }}
{% endmacro %}


{% macro render_iceberg_ddl(relation) -%}
  {%- set external_volume = config.get('external_volume') -%}
  {# S3 treats subpaths with or without a trailing '/' as functionally equivalent #}
  {%- set subpath = config.get('base_location_subpath') -%}
  {%- set base_location = '_dbt/' ~ relation.schema ~ '/' ~ relation.name ~ (('/' ~ subpath) if subpath else '') -%}

  external_volume = '{{ external_volume }}'
  catalog = 'snowflake'
  base_location = '{{ base_location }}'
{% endmacro %}
