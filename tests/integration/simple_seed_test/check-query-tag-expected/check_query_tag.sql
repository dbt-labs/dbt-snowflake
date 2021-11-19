

with query as (

    -- check that the current value for id=1 is red
    select case when (
        select count(*)
        from table(information_schema.query_history_by_user())
        where QUERY_TAG = '{{ var('query_tag') }}'
    ) > 1 then 0 else 1 end as failures

)

select *
from query
where failures = 1
