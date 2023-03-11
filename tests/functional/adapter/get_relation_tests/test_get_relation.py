import pytest

from dbt.tests.util import run_dbt

from tests.functional.adapter.get_relation_tests import macros


class TestGetRelation:

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "quoting": {
                "database": False,
                "schema": False,
                "identifier": False,
            }
        }

    @pytest.fixture(scope="class")
    def unique_schema(self, request, prefix) -> str:
        test_file = request.module.__name__
        # We only want the last part of the name
        test_file = test_file.split(".")[-1]
        unique_schema = f"{prefix}_{test_file}"
        # SnowflakeAdapter._match_kwargs() defaults to upper() whereas BaseAdapter defaults to lower()
        return unique_schema.upper()

    @pytest.fixture(scope="class")
    def models(self):
        return {"FACT.sql": "select 1 as my_column"}

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "get_relation.sql": macros.MACRO_GET_RELATION,
            "is_relation.sql": macros.MACRO_IS_RELATION,
            "check_get_relation_is_relation.sql": macros.MACRO_CHECK_GET_RELATION_IS_RELATION,
        }

    @pytest.fixture(scope="class", autouse=True)
    def setup_class(self, project):
        run_dbt(["run"])

    def test_get_relation(self, project):
        relation = project.adapter.execute_macro("get_relation")
        assert relation.get("metadata", {}).get("type") == "SnowflakeRelation"

    def test_is_relation(self, project):
        relation = project.adapter.execute_macro("get_relation")
        assert project.adapter.execute_macro("is_relation", kwargs={"obj": relation}) is True

    def test_check_get_relation_is_relation(self, project):
        relation = project.adapter.execute_macro("check_get_relation_is_relation")
        assert relation.get("metadata", {}).get("type") == "SnowflakeRelation"
