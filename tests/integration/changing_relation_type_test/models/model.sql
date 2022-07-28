{{ config(materialized=var('materialized')) }}

select '{{ var("materialized") }}' as materialization

{% if var('materialized') == 'incremental' and is_incremental() %}
    where 'abc' != (select max(materialization) from {{ this }})
{% endif %}

{% if var('materialized') ==  'materializedview' %}
from change_relation_type_tbl
{% endif %}