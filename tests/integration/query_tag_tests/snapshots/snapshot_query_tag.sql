{% snapshot snapshot_query_tag %}
    {{
        config(
            target_database=database,
            target_schema=schema,
            unique_key='id',
            strategy='check',
            check_cols=['color'],
        )
    }}
    select 1 as id, 'blue' as color
{% endsnapshot %}
