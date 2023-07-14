from typing import Optional

from dbt.adapters.relation.models import Relation
from dbt.tests.util import get_model_file, set_model_file


def swap_target_lag(project, my_dynamic_table):
    initial_model = get_model_file(project, my_dynamic_table)
    new_model = initial_model.replace("target_lag='2 minutes'", "target_lag='5 minutes'")
    set_model_file(project, my_dynamic_table, new_model)


def swap_dynamic_table_to_table(project, my_dynamic_table):
    initial_model = get_model_file(project, my_dynamic_table)
    new_model = initial_model.replace("materialized='dynamic_table'", "materialized='table'")
    set_model_file(project, my_dynamic_table, new_model)


def swap_dynamic_table_to_view(project, my_dynamic_table):
    initial_model = get_model_file(project, my_dynamic_table)
    new_model = initial_model.replace("materialized='dynamic_table'", "materialized='view'")
    set_model_file(project, my_dynamic_table, new_model)


def query_relation_type(project, relation: Relation) -> Optional[str]:
    sql = f"""
        select
            case
                when table_type = 'BASE TABLE' then 'table'
                when table_type = 'VIEW' then 'view'
                when table_type = 'EXTERNAL TABLE' then 'external_table'
                when table_type is null then 'dynamic_table'
            end as relation_type
        from information_schema.tables
        where table_name like '{relation.name.upper()}'
        and table_schema like '{relation.schema_name.upper()}'
        and table_catalog like '{relation.database_name.upper()}'
    """
    results = project.run_sql(sql, fetch="one")
    if results is None or len(results) == 0:
        return None
    elif len(results) > 1:
        raise ValueError(f"More than one instance of {relation.name} found!")
    else:
        return results[0].lower()


def query_row_count(project, relation: Relation) -> int:
    sql = f"select count(*) from {relation.fully_qualified_path};"
    return project.run_sql(sql, fetch="one")[0]


def query_target_lag(project, relation: Relation) -> bool:
    sql = f"""
        show dynamic tables
            like '{ relation.name }'
            in schema { relation.schema.fully_qualified_path }
        ;
        select "target_lag"
        from table(result_scan(last_query_id()))
    """
    return project.run_sql(sql, fetch=True)
