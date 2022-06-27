
{% if var('dependent_type', 'view') == 'view' %}
    {{ config(materialized='view') }}
{% elif var('dependent_type') == 'materializedview' %}
    {{ config(materialized='materializedview') }}
{% else %}
{{ config(materialized='table') }}
{% endif %}

select * from {{ ref('base_table') }}
