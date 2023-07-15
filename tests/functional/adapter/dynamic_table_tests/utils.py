from typing import Optional

from dbt.adapters.snowflake.relation import SnowflakeRelation


def query_relation_type(project, relation: SnowflakeRelation) -> Optional[str]:
    sql = f"""
        select
            case
                when table_type = 'BASE TABLE' then 'table'
                when table_type = 'VIEW' then 'view'
                when table_type = 'EXTERNAL TABLE' then 'external_table'
                when table_type is null then 'dynamic_table'
            end as relation_type
        from information_schema.tables
        where table_name like '{relation.identifier.upper()}'
        and table_schema like '{relation.schema.upper()}'
        and table_catalog like '{relation.database.upper()}'
    """
    results = project.run_sql(sql, fetch="one")
    if results is None or len(results) == 0:
        return None
    elif len(results) > 1:
        raise ValueError(f"More than one instance of {relation.name} found!")
    else:
        return results[0].lower()
