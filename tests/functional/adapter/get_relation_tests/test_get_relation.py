"""
This test suite is the result of GitHub issue #dbt-core/7024: https://github.com/dbt-labs/dbt-core/issues/7024
"""
import pytest

from dbt.tests.util import run_dbt
from dbt.exceptions import CompilationError

from tests.functional.adapter.get_relation_tests import macros, models


class GetRelationBase:

    @pytest.fixture(scope="class")
    def project_config_update(self):
        """
        There was initial concern that the quote policy was to blame, though that is unlikely after investigation
        """
        return {
            "quoting": {
                "database": False,
                "schema": False,
                "identifier": False,
            }
        }

    @pytest.fixture(scope="class")
    def unique_schema(self, request, prefix) -> str:
        """
        The user specified the schema in their post, hence we hard code it here to use in fixtures.
        This must match the macro `macros.GET_RELATION`.
        """
        return "DBT_CORE_ISSUE_7024"


class TestGetRelationDirectCall(GetRelationBase):
    """
    Loads only a dummy model and all macros. Everything is independent so it can all be checked before chaining.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {"FACT.sql": models.FACT}

    @pytest.fixture(scope="class")
    def macros(self):

        def is_to_assert(macro):
            # alias the "assert_relation" macros so that they can be loaded alongside the "is_relation" macros
            return macro.replace("is_relation", "assert_relation")

        return {
            "get_relation.sql": macros.GET_RELATION,
            "is_relation.sql": macros.IS_RELATION,
            "assert_relation.sql": is_to_assert(macros.IS_RELATION),
            "check_get_relation_is_relation.sql": macros.CHECK_GET_RELATION_IS_RELATION,
            "check_get_relation_assert_relation.sql": is_to_assert(macros.CHECK_GET_RELATION_IS_RELATION),
        }

    @pytest.fixture(scope="class", autouse=True)
    def setup(self, project):
        run_dbt()

    @pytest.fixture(scope="class")
    def dummy_relation(self, project):
        return project.adapter.execute_macro("get_relation")

    def test_get_relation(self, project, dummy_relation):
        assert dummy_relation.get("metadata", {}).get("type") == "SnowflakeRelation"

    def test_is_relation(self, project, dummy_relation):
        assert project.adapter.execute_macro("is_relation", kwargs={"obj": dummy_relation})

    def test_assert_relation(self, project, dummy_relation):
        assert project.adapter.execute_macro("assert_relation", kwargs={"obj": dummy_relation})

    def test_check_get_relation_is_relation(self, project):
        assert project.adapter.execute_macro("check_get_relation_is_relation")

    def test_check_get_relation_assert_relation(self, project):
        """
        This test case is the origin for this test module; however, it passes, hence the troubleshooting in the
        remainder of the module.
        """
        assert project.adapter.execute_macro("check_get_relation_assert_relation")


class GetRelationBaseWithModels(GetRelationBase):
    """
    Loads all three models. The models are the same for both test classes, only `is_relation`/`assert_relation`
    macro changes. Both are aliased as `is_relation`.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "FACT.sql": models.FACT,
            "INVOKE_IS_RELATION.sql": models.INVOKE_IS_RELATION,
            "INVOKE_IS_RELATION_WITH_REF.sql": models.INVOKE_IS_RELATION_WITH_REF,
        }

    @staticmethod
    def results_from_invoke_table(project, with_ref: bool):
        invoke_table = f"{project.database}.DBT_CORE_ISSUE_7024.INVOKE_IS_RELATION"
        if with_ref:
            invoke_table += "_WITH_REF"
        return project.run_sql(f"""select * from {invoke_table}""", fetch="all")

    @pytest.fixture(scope="class")
    def fact_table(self, project):
        return f"{project.database}.DBT_CORE_ISSUE_7024.FACT"

    @pytest.fixture(scope="class")
    def quoted_fact_table(self, project):
        return f'"{project.database}"."DBT_CORE_ISSUE_7024"."FACT"'


class TestGetRelationIsRelationModelCall(GetRelationBaseWithModels):
    """
    Loads a version of `is_relation()` that doesn't throw an error when the check fails
    """

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "get_relation.sql": macros.GET_RELATION,
            "is_relation.sql": macros.IS_RELATION,  # is_relation doesn't throw error
            "check_get_relation_is_relation.sql": macros.CHECK_GET_RELATION_IS_RELATION,
        }

    @pytest.fixture(scope="class", autouse=True)
    def setup(self, project):
        run_dbt()

    def test_get_relation_with_ref(self, project, fact_table):
        """
        When we include the ref statement in the model (even though we don't use anything from that relation),
        the macro executes *after* the model is created, hence INVOKE_REF picks up the existence of FACT via
        the `get_relation` macro.
        """
        results = self.results_from_invoke_table(project, True)
        assert results == [(fact_table, True)]

    def test_get_relation_without_ref(self, project):
        """
        When we don't include the ref statement in the model, the macro executes *before* the model is created,
        hence INVOKE_NO_REF *doesn't* pick up the existence of FACT via the `get_relation` macro.

        Note: If this happens to run after `test_get_relation_without_ref_called_twice`, it will fail because
        `FACT` will exist at that point. It's the same scenario as `test_get_relation_without_ref_called_twice`
        because the `project` fixture is class-scoped.

        It's worth calling out the above as it essentially means inserting macros in models that depend on other
        models is effectively violating idempotency.
        """
        results = self.results_from_invoke_table(project, False)
        assert results == [("None", False)]

    def test_get_relation_without_ref_called_twice(self, project, quoted_fact_table):
        """
        When we don't include the ref statement in the model, the macro executes *before* the model is created,
        hence INVOKE_NO_REF *doesn't* pick up the existence of FACT via the `get_relation` macro.

        However, running dbt a second time will then pick it up because now it exists. Though surprisingly,
        it now returns a quoted relation name, unlike `test_get_relation_with_ref`.
        """
        run_dbt()
        results = self.results_from_invoke_table(project, False)
        assert results == [(quoted_fact_table, True)]


class TestGetRelationAssertRelationModelCall(GetRelationBaseWithModels):
    """
    Loads a version of `is_relation()` that throws an error when the check fails
    """

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "get_relation.sql": macros.GET_RELATION,
            "is_relation.sql": macros.ASSERT_RELATION,  # assert_relation throws error
            "check_get_relation_is_relation.sql": macros.CHECK_GET_RELATION_IS_RELATION,
        }

    def test_cannot_run_dbt(self, project):
        """
        If we include the version that throws an exception, we simply can't run dbt
        """
        with pytest.raises(CompilationError) as exception_results:
            run_dbt(expect_pass=False)
        assert "Macro expected a Relation but received the value: None" in str(exception_results.value)
