-- depends on: {{ ref('view_model_query_tag') }}
-- depends on: {{ ref('table_model_query_tag') }}
-- depends on: {{ ref('incremental_model_query_tag') }}
-- depends on: {{ ref('seed_query_tag') }}
-- depends on: {{ ref('snapshot_query_tag') }}

-- We expect to find at least one tagged query for each of the 5 resources that just ran

{{ config(
    error_if = "< 5",
    warn_if = "< 5"
) }}

select query_tag, array_agg(query_type)
from table(information_schema.query_history_by_user())
where query_tag in (
  '{{ var("query_tag") }}_view',
  '{{ var("query_tag") }}_table',
  '{{ var("query_tag") }}_incremental',
  '{{ var("query_tag") }}_seed',
  '{{ var("query_tag") }}_snapshot'
)
group by 1
