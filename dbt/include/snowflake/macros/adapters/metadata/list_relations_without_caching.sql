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
