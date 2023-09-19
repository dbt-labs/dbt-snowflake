from typing import Dict, Set, Tuple


def get_relation_summary_in_schema(project, schema: str) -> Set[Tuple]:
    """
    Returns a summary like this:
        {
            ("my_table", "table", 0),
            ("my_view", "view", 1),
        }
    """
    sql = """
    We can't get the relation type in tests right now because it requires a multi-statement sql execution.
    This needs to be solved prior to automating these tests. This will also resolve the same issue for DT tests.
    """
    relation_types = project.run_sql(sql, fetch="all")

    results = set()
    for relation_name, relation_type in relation_types:
        row_count_sql = f"select count(*) from {schema}.{relation_name}"
        row_count = project.run_sql(row_count_sql, fetch="one")[0]
        summary = (relation_name, relation_type, row_count)
        results.add(summary)

    return results


def insert_record(project, schema: str, table_name: str, record: Dict[str, str]):
    # postgres only supports schema names of 63 characters
    # a schema with a longer name still gets created, but the name gets truncated
    schema_name = schema[:63]
    field_names, field_values = [], []
    for field_name, field_value in record.items():
        field_names.append(field_name)
        field_values.append(f"'{field_value}'")
    field_name_clause = ", ".join(field_names)
    field_value_clause = ", ".join(field_values)

    sql = f"""
    insert into {schema_name}.{table_name} ({field_name_clause})
    values ({field_value_clause})
    """
    project.run_sql(sql)


def delete_record(project, schema: str, table_name: str, record: Dict[str, str]):
    schema_name = schema[:63]
    where_clause = " and ".join(
        [f"{field_name} = '{field_value}'" for field_name, field_value in record.items()]
    )
    sql = f"""
    delete from {schema_name}.{table_name}
    where {where_clause}
    """
    project.run_sql(sql)
