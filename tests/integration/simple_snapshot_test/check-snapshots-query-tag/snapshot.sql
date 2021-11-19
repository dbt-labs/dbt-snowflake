{% snapshot snapshot_query_tag %}
    {{
        config(
            target_database=database,
            target_schema=schema,
            unique_key='id',
            strategy='check',
            check_cols=['color'],
            query_tag=var('query_tag')
        )
    }}
    select * from {{target.database}}.{{schema}}.seed
{% endsnapshot %}
