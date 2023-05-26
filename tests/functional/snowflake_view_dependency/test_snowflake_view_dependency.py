import pytest
from dbt.tests.util import run_dbt, check_relations_equal

_MODELS__DEPENDENT_MODEL_SQL = """
{% if var('dependent_type', 'view') == 'view' %}
    {{ config(materialized='view') }}
{% else %}
    {{ config(materialized='table') }}
{% endif %}

select * from {{ ref('base_table') }}
"""


_MODELS__BASE_TABLE_SQL = """
{{ config(materialized='table') }}
select *
    {% if var('add_table_field', False) %}
        , 1 as new_field
    {% endif %}

from {{ ref('people') }}
"""

_SEEDS__PEOPLE_CSV = """id,name
1,Drew
2,Jake
3,Connor
"""


class TestSnowflakeLateBindingViewDependency:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "dependent_model.sql": _MODELS__DEPENDENT_MODEL_SQL,
            "base_table.sql": _MODELS__BASE_TABLE_SQL,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"people.csv": _SEEDS__PEOPLE_CSV}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {
                "quote_columns": False,
            },
            "quoting": {"schema": False, "identifier": False},
        }

    @pytest.fixture(scope="class", autouse=True)
    def setup_method(self, project):
        results = run_dbt(["seed"])
        assert len(results) == 1
        results = run_dbt(["run"])
        assert len(results) == 2
        check_relations_equal(project.adapter, ["PEOPLE", "BASE_TABLE"])
        check_relations_equal(project.adapter, ["PEOPLE", "DEPENDENT_MODEL"])

    def check_result(self, project, results, expected_types):
        for result in results:
            node_name = result.node.name
            node_type = result.node.config.materialized
            assert node_type == expected_types[node_name]

    """
    Snowflake views are not bound to the relations they select from. A Snowflake view
    can have entirely invalid SQL if, for example, the table it selects from is dropped
    and recreated with a different schema. In these scenarios, Snowflake will raise an error if:
    1) The view is queried
    2) The view is altered

    dbt's logic should avoid running these types of queries against views in situations
    where they _may_ have invalid definitions. These tests assert that views are handled
    correctly in various different scenarios
    """

    def test__snowflake__changed_table_schema_for_downstream_view(self, project):
        run_dbt(["seed"])
        # Change the schema of base_table, assert that dependent_model doesn't fail
        results = run_dbt(["run", "--vars", "{add_table_field: true, dependent_type: view}"])
        assert len(results) == 2
        check_relations_equal(project.adapter, ["BASE_TABLE", "DEPENDENT_MODEL"])

    """
    This test is similar to the one above, except the downstream model starts as a view, and
    then is changed to be a table. This checks that the table materialization does not
    errantly rename a view that might have an invalid definition, which would cause an error
    """

    def test__snowflake__changed_table_schema_for_downstream_view_changed_to_table(self, project):
        run_dbt(["seed"])
        results = run_dbt(["run"])
        expected_types = {"base_table": "table", "dependent_model": "view"}
        # ensure that the model actually was materialized as a view
        self.check_result(project, results, expected_types)
        results = run_dbt(["run", "--vars", "{add_table_field: true, dependent_type: table}"])
        assert len(results) == 2
        check_relations_equal(project.adapter, ["BASE_TABLE", "DEPENDENT_MODEL"])
        expected_types = {"base_table": "table", "dependent_model": "table"}
        # ensure that the model actually was materialized as a table
        self.check_result(project, results, expected_types)
