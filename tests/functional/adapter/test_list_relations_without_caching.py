import pytest
from dbt.tests.util import run_dbt

_TABLE_1_SQL = """
{{ config(materialized='table') }}

select 1 as id
""".lstrip()

_VIEW_X_SQL = """
select id from {{ref('my_model_0')}}
""".lstrip()


class TestListRelationsWithoutCaching:
    @pytest.fixture(scope="class")
    def models(self):
        my_models = {"my_model_0.sql": _TABLE_1_SQL}
        for view in range(1, 10100):
            my_models.update({f"my_model_{view}.sql": _VIEW_X_SQL})

        return my_models

    def test__snowflake__list_relations_without_caching(self, project):
        # purpose of the first run is to create over 10K objects in the target schema
        result = run_dbt()
        # purpose of the second run is to load test the list_relations_without_caching macro
        # we do not need to RUN all the models this time, only list their targets
        # as my_model_0 and all others are built into the same schema
        result = run_dbt(["run", "-s", "my_model_0"])
        assert len(result) == 1
