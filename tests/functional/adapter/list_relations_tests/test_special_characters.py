import pytest
from dbt.tests.util import run_dbt


TABLE_BASE_SQL = """
-- models/my_model.sql
{{ config(schema = '1_contains_special*character$') }}
select 1 as id
"""


class TestSpecialCharactersInSchema:

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"quoting": {"schema": True}}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": TABLE_BASE_SQL,
        }

    def test_schema_with_special_chars(self, project):
        run_dbt(["run", "-s", "my_model"])
