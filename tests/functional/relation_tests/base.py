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


MACRO__GET_RENAME_SQL = """
{% macro test__get_rename_sql(database, schema, identifier, relation_type, new_name) -%}
    {%- set relation = adapter.Relation.create(database=database, schema=schema, identifier=identifier, type=relation_type) -%}
    {% call statement('test__get_rename_sql') -%}
        {{ get_rename_sql(relation, new_name) }}
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
            "my_view.sql": VIEW,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        yield {"test__get_rename_sql.sql": MACRO__GET_RENAME_SQL}

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
