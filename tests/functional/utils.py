from typing import Any, Dict, Optional

from dbt.tests.util import (
    get_connection,
    get_model_file,
    relation_from_name,
    set_model_file,
)

from dbt.adapters.snowflake.relation_configs import SnowflakeDynamicTableConfig


def query_relation_type(project, name: str) -> Optional[str]:
    relation = relation_from_name(project.adapter, name)
    sql = f"""
        select
            case table_type
                when 'BASE TABLE' then iff(is_dynamic = 'YES', 'dynamic_table', 'table')
                when 'VIEW' then 'view'
                when 'EXTERNAL TABLE' then 'external_table'
            end as relation_type
        from information_schema.tables
        where table_name like '{relation.identifier.upper()}'
        and table_schema like '{relation.schema.upper()}'
        and table_catalog like '{relation.database.upper()}'
    """
    results = project.run_sql(sql, fetch="all")

    assert len(results) > 0, f"Relation {relation} not found"
    assert len(results) == 1, f"Multiple relations found"

    return results[0][0].lower()


def query_row_count(project, name: str) -> int:
    relation = relation_from_name(project.adapter, name)
    sql = f"select count(*) from {relation}"
    return project.run_sql(sql, fetch="one")[0]


def insert_record(project, name: str, record: Dict[str, Any]):
    relation = relation_from_name(project.adapter, name)
    column_names = ", ".join(record.keys())
    values = ", ".join(
        [f"'{value}'" if isinstance(value, str) else f"{value}" for value in record.values()]
    )
    sql = f"insert into {relation} ({column_names}) values ({values})"
    project.run_sql(sql)


def update_model(project, name: str, model: str) -> str:
    relation = relation_from_name(project.adapter, name)
    original_model = get_model_file(project, relation)
    set_model_file(project, relation, model)
    return original_model


def describe_dynamic_table(project, name: str) -> Optional[SnowflakeDynamicTableConfig]:
    macro = "snowflake__describe_dynamic_table"
    dynamic_table = relation_from_name(project.adapter, name)
    kwargs = {"relation": dynamic_table}
    with get_connection(project.adapter):
        results = project.adapter.execute_macro(macro, kwargs=kwargs)

    assert len(results["dynamic_table"].rows) > 0, f"Dynamic table {dynamic_table} not found"
    found = len(results["dynamic_table"].rows)
    names = ", ".join([table.get("name") for table in results["dynamic_table"].rows])
    assert found == 1, f"Multiple dynamic tables found: {names}"

    return SnowflakeDynamicTableConfig.from_relation_results(results)


def refresh_dynamic_table(project, name: str) -> None:
    macro = "snowflake__refresh_dynamic_table"
    dynamic_table = relation_from_name(project.adapter, name)
    kwargs = {"relation": dynamic_table}
    with get_connection(project.adapter):
        project.adapter.execute_macro(macro, kwargs=kwargs)
