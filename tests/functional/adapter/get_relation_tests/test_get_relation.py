import pytest

from dbt.tests.util import run_dbt

from tests.functional.adapter.get_relation_tests import macros


_MODEL_FACT = "select 1 as my_column"


_MODEL_INVOKE_REF = """
select
    '{{ get_relation(
            target.database,
            target.schema,
            "FACT"
    ) }}' as get_relation,
    {{ check_get_relation_is_relation(
            target.database,
            target.schema,
            "FACT"
    ) }} as is_relation
from {{ ref('FACT') }}
"""


_MODEL_INVOKE_NO_REF = """
select
    '{{ get_relation(
            target.database,
            target.schema,
            "FACT"
    ) }}' as get_relation,
    {{ check_get_relation_is_relation(
            target.database,
            target.schema,
            "FACT"
    ) }} as is_relation
"""


class GetRelationBase:

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
    def macros(self):
        return {
            "get_relation.sql": macros.MACRO_GET_RELATION,
            "is_relation.sql": macros.MACRO_IS_RELATION,
            "check_get_relation_is_relation.sql": macros.MACRO_CHECK_GET_RELATION_IS_RELATION,
        }

    @pytest.fixture(scope="class", autouse=True)
    def setup(self, project):
        run_dbt()


class TestGetRelationDirectCall(GetRelationBase):

    @pytest.fixture(scope="class")
    def models(self):
        return {"FACT.sql": _MODEL_FACT}

    @pytest.fixture(scope="function")
    def dummy_model(self, project):
        return {
            "database": project.database,
            "schema": project.test_schema,
            "identifier": "FACT",
        }

    @pytest.fixture(scope="function")
    def dummy_relation(self, project, dummy_model):
        return project.adapter.execute_macro("get_relation", kwargs=dummy_model)

    def test_get_relation(self, project, dummy_model):
        relation = project.adapter.execute_macro("get_relation", kwargs=dummy_model)
        assert relation.get("metadata", {}).get("type") == "SnowflakeRelation"

    def test_is_relation(self, project, dummy_relation):
        assert project.adapter.execute_macro("is_relation", kwargs={"obj": dummy_relation})

    def test_check_get_relation_is_relation(self, project, dummy_model):
        assert project.adapter.execute_macro("check_get_relation_is_relation", kwargs=dummy_model)


class TestGetRelationModelCall(GetRelationBase):

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "FACT.sql": _MODEL_FACT,
            "INVOKE_REF.sql": _MODEL_INVOKE_REF,
            "INVOKE_NO_REF.sql": _MODEL_INVOKE_NO_REF,
        }

    def test_get_relation_with_ref(self, project):
        """
        When we include the ref statement in the model (even though we don't use anything from that relation),
        the macro executes *after* the model is created, hence INVOKE_REF picks up the existence of FACT via
        the `get_relation` macro.
        """
        invoke_table = f"{project.database}.{project.test_schema}.INVOKE_REF"
        fact_table = f"{project.database}.{project.test_schema}.FACT"

        results = project.run_sql(f"""select * from {invoke_table}""", fetch="all")
        assert len(results) == 1
        assert results[0] == (fact_table, True)

    def test_get_relation_with_no_ref(self, project):
        """
        When we don't include the ref statement in the model, the macro executes *before* the model is created,
        hence INVOKE_NO_REF *doesn't* pick up the existence of FACT via the `get_relation` macro.
        """
        invoke_table = f"{project.database}.{project.test_schema}.INVOKE_NO_REF"
        fact_table = "None"

        results = project.run_sql(f"""select * from {invoke_table}""", fetch="all")
        assert len(results) == 1
        assert results[0] == (fact_table, False)

        run_dbt(["run", "-s", "INVOKE_NO_REF"])
        # note the extra quotes, different from above
        fact_table = f'"{project.database}"."{project.test_schema}"."FACT"'

        results = project.run_sql(f"""select * from {invoke_table}""", fetch="all")
        assert len(results) == 1
        assert results[0] == (fact_table, True)
