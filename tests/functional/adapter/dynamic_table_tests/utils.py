from typing import Optional

import agate
from dbt.adapters.base import BaseAdapter
from dbt.tests.util import get_connection

from dbt.adapters.snowflake.relation import SnowflakeRelation


def query_relation_type(project, relation: SnowflakeRelation) -> Optional[str]:
    sql = f"""
        select
            case
                when t.table_type = 'BASE TABLE' and dt.completion_target is not null then 'dynamic_table'
                when t.table_type = 'BASE TABLE' then 'table'
                when t.table_type = 'VIEW' then 'view'
                when t.table_type = 'EXTERNAL TABLE' then 'external_table'
            end as relation_type
        from
            information_schema.tables t
        left join
            information_schema.dynamic_table_refresh_history dt on t.table_catalog = dt.table_catalog
            and t.table_schema = dt.schema_name
            and t.table_name = dt.name
        where
            t.table_name like '{relation.identifier.upper()}'
            and t.schema_name like '{relation.schema.upper()}'
            and t.table_catalog like '{relation.database.upper()}'
    """
    results = project.run_sql(sql, fetch="one")
    if results is None or len(results) == 0:
        return None
    elif len(results) > 1:
        raise ValueError(f"More than one instance of {relation.name} found!")
    else:
        return results[0].lower()


def query_target_lag(adapter, dynamic_table: SnowflakeRelation) -> Optional[str]:
    config = describe_dynamic_table(adapter, dynamic_table)
    return config.get("target_lag")


def query_warehouse(adapter, dynamic_table: SnowflakeRelation) -> Optional[str]:
    config = describe_dynamic_table(adapter, dynamic_table)
    return config.get("warehouse")


def describe_dynamic_table(adapter: BaseAdapter, dynamic_table: SnowflakeRelation) -> agate.Row:
    with get_connection(adapter):
        macro_results = adapter.execute_macro(
            "snowflake__describe_dynamic_table", kwargs={"relation": dynamic_table}
        )
    config = macro_results["dynamic_table"]
    return config.rows[0]
