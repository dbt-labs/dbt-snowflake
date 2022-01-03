{%- macro snowflake__get_incremental_predicates(target_relation, incremental_strategy, unique_key, user_predicates=none, partitions=none) -%}
    {%- if incremental_strategy == 'merge' -%}
        {%- if unique_key -%}
            {%- set match_criteria -%}
                DBT_INTERNAL_SOURCE.{{ unique_key }} = DBT_INTERNAL_DEST.{{ unique_key }}  
            {%- endset -%}
        {%- else -%}
            {%- set match_criteria %}
                FALSE
            {%- endset -%}
        {%- endif -%}

        {%- if user_predicates -%}
            {%- set filter_criteria %}
                {%- for condition in user_predicates -%} and DBT_INTERNAL_DEST.{{ condition.source_col }} {{ condition.expression }} {% endfor -%}
            {%- endset -%}
        {%- endif -%}
    {%- elif incremental_strategy == 'delete+insert' -%}
        {%- if user_predicates -%}
            {%- set filter_criteria %}
                {%- for condition in user_predicates -%} and {{ target_relation.name }}.{{ condition.source_col }} {{ condition.expression }} {% endfor -%}
            {%- endset -%}
        {%- endif -%}
    {%- endif -%}

    {{ match_criteria }}
    {{ filter_criteria }}

{%- endmacro -%}