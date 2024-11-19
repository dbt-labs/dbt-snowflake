import os

import pytest

from dbt_common.exceptions import CompilationError
from dbt.tests.util import run_dbt

"""
Testing rationale:
- snowflake SHOW TERSE OBJECTS command returns at max 10K objects in a single call
- when dbt attempts to write into a schema with more than 10K objects, compilation will fail
  unless we paginate the result
- we default pagination to 10 pages, but users want to configure this
  - we instead use that here to force failures by making it smaller
"""


TABLE = """
{{ config(materialized='table') }}
select 1 as id
"""


VIEW = """
{{ config(materialized='view') }}
select id from {{ ref('my_model_base') }}
"""


DYNAMIC_TABLE = (
    """
{{ config(
    materialized='dynamic_table',
    target_lag='1 hour',
    snowflake_warehouse='"""
    + os.getenv("SNOWFLAKE_TEST_WAREHOUSE")
    + """',
) }}

select id from {{ ref('my_model_base') }}
"""
)


class BaseConfig:
    VIEWS = 90
    DYNAMIC_TABLES = 10

    @pytest.fixture(scope="class")
    def models(self):
        my_models = {"my_model_base.sql": TABLE}
        for view in range(0, self.VIEWS):
            my_models[f"my_model_{view}.sql"] = VIEW
        for dynamic_table in range(0, self.DYNAMIC_TABLES):
            my_models[f"my_dynamic_table_{dynamic_table}.sql"] = DYNAMIC_TABLE
        return my_models

    @pytest.fixture(scope="class", autouse=True)
    def setup(self, project):
        run_dbt(["run"])

    def test_list_relations(self, project):
        kwargs = {"schema_relation": project.test_schema}
        with project.adapter.connection_named("__test"):
            relations = project.adapter.execute_macro(
                "snowflake__list_relations_without_caching", kwargs=kwargs
            )
        assert len(relations) == self.VIEWS + self.DYNAMIC_TABLES + 1


class TestListRelationsWithoutCachingSmall(BaseConfig):
    pass


class TestListRelationsWithoutCachingLarge(BaseConfig):
    @pytest.fixture(scope="class")
    def profiles_config_update(self):
        return {
            "flags": {
                "list_relations_per_page": 10,
                "list_relations_page_limit": 20,
            }
        }


class TestListRelationsWithoutCachingTooLarge(BaseConfig):

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {
                "list_relations_per_page": 10,
                "list_relations_page_limit": 5,
            }
        }

    def test_list_relations(self, project):
        kwargs = {"schema_relation": project.test_schema}
        with project.adapter.connection_named("__test"):
            with pytest.raises(CompilationError) as error:
                project.adapter.execute_macro(
                    "snowflake__list_relations_without_caching", kwargs=kwargs
                )
            assert "list_relations_per_page" in error.value.msg
            assert "list_relations_page_limit" in error.value.msg

    def test_on_run(self, project):
        with pytest.raises(CompilationError) as error:
            run_dbt(["run"])
        assert "list_relations_per_page" in error.value.msg
        assert "list_relations_page_limit" in error.value.msg
