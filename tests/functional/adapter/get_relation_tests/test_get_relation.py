from typing import Mapping

import pytest

from dbt.tests.util import run_dbt

from dbt.adapters.snowflake.relation import SnowflakeRelation

from tests.functional.adapter.get_relation_tests import macros


class GetRelationBase:

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "quoting": {
                "database": True,
                "schema": True,
                "identifier": True,
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
        """
        dummy model to be checked for existence by `is_relation` and to be returned by `get_relation`
        """
        return {"FACT.sql": "select 1 as my_column"}

    @pytest.fixture(scope="class", autouse=True)
    def setup_class(self, project):
        run_dbt(["run"])

    @pytest.fixture(scope="function")
    def ad_hoc_table(self, project):
        """
        Manually create a table in the test schema, not using a model

        This behaves differently from a table created via a model for `get_relation`
        """
        source_table = "FACT"
        test_table = "AD_HOC"

        create_sql = f"""
            create or replace table {project.test_schema}.{test_table} as
            select * from {project.test_schema}.{source_table}
        """
        verify_sql = f"select * from {project.test_schema}.{test_table}"
        drop_sql = f"drop table if exists {project.test_schema}.{test_table}"

        project.run_sql(create_sql)
        project.run_sql(verify_sql)
        yield
        project.run_sql(drop_sql)


class TestGetRelationPython(GetRelationBase):

    @pytest.mark.parametrize("table_name,expected_result", [
        ("FACT", True),
        ("AD_HOC", True),
    ])
    def test_table_exists_with_standard_sql(self, project, ad_hoc_table, table_name, expected_result):
        """
        assert the table exists by trying to query it

        This basically tests the setup and fixtures
        """
        verify_sql = f"select * from {project.test_schema}.{table_name}"
        records = project.run_sql(verify_sql, fetch='one')
        table_exists = (len(records) > 0)
        assert table_exists == expected_result

    @pytest.mark.parametrize("table_name,expected_result", [
        ("FACT", True),         # this exists and should be returned
        ("AD_HOC", False),  # this exists but should not be returned
    ])
    def test_table_exists_with_get_relation(self, project, ad_hoc_table, table_name, expected_result):
        """
        assert the table exists using `get_relation`

        `get_relation` will return a relation if the relation exists as a model
        `get_relation` will return None if the relation does not exist as a model, even if it exists as a table/view
        """
        get_relation = project.adapter.get_relation(
            database=project.database,
            schema=project.test_schema,
            identifier=table_name
        )
        get_relation_returned_something = get_relation is not None
        assert get_relation_returned_something is expected_result

    @pytest.mark.parametrize("table_name,expected_result", [
        ("FACT", True),
        ("AD_HOC", True),
    ])
    def test_table_exists_with_is_relation(self, project, ad_hoc_table, table_name, expected_result):
        """
        assert the table exists using `is_relation`

        `is_relation` will return True as long as `relation` is an instance of `BaseRelation`
        """
        relation = SnowflakeRelation.create(
            database=project.database,
            schema=project.test_schema,
            identifier=table_name
        )

        is_relation = (
            isinstance(relation, Mapping) and
            relation.get("metadata", "").get("type", "").endswith("Relation")
        )
        assert is_relation == expected_result


class TestGetRelationDBT(GetRelationBase):

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "get_relation.sql": macros.MACRO_GET_RELATION,
            "is_relation.sql": macros.MACRO_IS_RELATION,
        }

    @pytest.fixture(scope="class")
    def tests(self):
        """
        These tests mirror `test_table_exists_with_get_relation` and `test_table_exists_with_is_relation`
        on `TestGetRelationPython` for `FACT` and `AD_HOC`
        """
        return {
            "test_get_relation_fact.sql": macros.TEST_GET_RELATION_FACT,
            "test_get_relation_ad_hoc.sql": macros.TEST_GET_RELATION_AD_HOC,
            "test_is_relation_fact.sql": macros.TEST_IS_RELATION_FACT,
            "test_is_relation_ad_hoc.sql": macros.TEST_IS_RELATION_AD_HOC,
        }

    def test_get_relation_dbt(self, project, ad_hoc_table):
        run_dbt(["test"])
