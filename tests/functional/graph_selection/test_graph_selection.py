import pytest
from dbt.tests.util import run_dbt
from tests.functional.graph_selection.fixtures import SelectionFixtures

selectors_yml = """
            selectors:
            - name: bi_selector
              description: This is a BI selector
              definition:
                method: tag
                value: bi
        """


@pytest.fixture
def selectors():
    return selectors_yml


def assert_correct_schemas(project):
    adapter = project.adapter
    with adapter.connection_named("__test"):
        exists = adapter.check_schema_exists(project.database, project.test_schema)
        assert exists

        schema = project.test_schema + "_and_then"
        exists = adapter.check_schema_exists(project.database, schema)
        assert not exists

def clear_schema(project):
    project.run_sql("drop schema if exists {schema} cascade")
    project.run_sql("create schema {schema}")


class TestSnowflakeGraphSelection(SelectionFixtures):
    @pytest.fixture(scope="class")
    def selectors(self):
        return selectors_yml


    def test_specific_model(self, project):
        run_dbt(["seed"])
        results = run_dbt(["run", "--select", "users"])
        assert len(results) == 1
        assert_correct_schemas(project)

    def test_specific_model_and_children(self, project):
        run_dbt(["seed"])
        results = run_dbt(["run", "--select", "users+"])
        assert len(results) == 4
        assert_correct_schemas(project)

    def test_specific_model_and_parents(self, project):
        run_dbt(["seed"])
        results = run_dbt(["run", "--select", "+users_rollup"])
        assert len(results) == 2
        assert_correct_schemas(project)

    def test_specific_model_with_exclusion(self, project):
        run_dbt(["seed"])
        results = run_dbt(
            [
                "run",
                "--select",
                "+users_rollup",
                "--exclude",
                "models/users_rollup.sql",
            ]
        )
        assert len(results) == 1
        assert_correct_schemas(project)

    def test_skip_intermediate(self, project):
        run_dbt(["seed"])
        results = run_dbt(['run', '--select', '@models/users.sql'])
        assert len(results) == 4

        results = run_dbt(['run', '--select', '@users', '--exclude', 'users_rollup'])
        assert len(results) == 3

        # make sure that users_rollup_dependency and users don't interleave
        users = [r for r in results if r.node.name == 'users'][0]
        dep = [r for r in results if r.node.name == 'users_rollup_dependency'][0]
        user_last_end = users.timing[1].completed_at
        dep_first_start = dep.timing[0].started_at
        assert user_last_end <= dep_first_start, f"dependency started before its transitive parent ({user_last_end} > {dep_first_start})"
