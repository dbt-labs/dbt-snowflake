import pytest

from dbt.tests.util import run_dbt, run_dbt_and_capture


SEED = """
id
0
1
2
""".strip()


TABLE = """
{{ config(materialized="table") }}
select * from {{ ref('my_seed') }}
"""


VIEW = """
{{ config(materialized="view") }}
select * from {{ ref('my_seed') }}
"""


MACRO__GET_CREATE_BACKUP_SQL = """
{% macro test__get_create_backup_sql(database, schema, identifier, relation_type) -%}
    {%- set relation = adapter.Relation.create(database=database, schema=schema, identifier=identifier, type=relation_type) -%}
    {% call statement('test__get_create_backup_sql') -%}
        {{ get_create_backup_sql(relation) }}
    {%- endcall %}
{% endmacro %}"""


MACRO__GET_RENAME_INTERMEDIATE_SQL = """
{% macro test__get_rename_intermediate_sql(database, schema, identifier, relation_type) -%}
    {%- set relation = adapter.Relation.create(database=database, schema=schema, identifier=identifier, type=relation_type) -%}
    {% call statement('test__get_rename_intermediate_sql') -%}
        {{ get_rename_intermediate_sql(relation) }}
    {%- endcall %}
{% endmacro %}"""


class RelationOperation:
    @pytest.fixture(scope="class")
    def seeds(self):
        yield {"my_seed.csv": SEED}

    @pytest.fixture(scope="class")
    def models(self):
        yield {
            "my_table.sql": TABLE,
            "my_table__dbt_tmp.sql": TABLE,
            "my_view.sql": VIEW,
            "my_view__dbt_tmp.sql": VIEW,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        yield {
            "test__get_create_backup_sql.sql": MACRO__GET_CREATE_BACKUP_SQL,
            "test__get_rename_intermediate_sql.sql": MACRO__GET_RENAME_INTERMEDIATE_SQL,
        }

    @pytest.fixture(scope="class", autouse=True)
    def setup(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])

    def assert_operation(self, project, operation, args, expected_statement):
        results, logs = run_dbt_and_capture(
            ["--debug", "run-operation", operation, "--args", str(args)]
        )
        assert len(results) == 1
        assert expected_statement in logs
