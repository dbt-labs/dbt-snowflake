import pytest

from pathlib import Path

from dbt.tests.util import run_dbt, rm_file, write_file, check_relations_equal

from dbt.tests.adapter.simple_copy.test_simple_copy import (
    SimpleCopySetup,
    SimpleCopyBase,
    EmptyModelsArentRunBase,
)

from tests.functional.adapter.simple_copy.fixtures import (
    _MODELS__INCREMENTAL_OVERWRITE,
    _MODELS__INCREMENTAL_UPDATE_COLS,
    _SEEDS__SEED_MERGE_EXPECTED,
    _SEEDS__SEED_MERGE_INITIAL,
    _SEEDS__SEED_MERGE_UPDATE,
    _SEEDS__SEED_UPDATE,
    _TESTS__GET_RELATION_QUOTING,
)


class TestSimpleCopyBase(SimpleCopyBase):
    @pytest.fixture(scope="class")
    def tests(self):
        return {"get_relation_test.sql": _TESTS__GET_RELATION_QUOTING}

    def test_simple_copy(self, project):
        super().test_simple_copy(project)
        run_dbt(["test"])


class TestSimpleCopyBaseQuotingOff(SimpleCopyBase):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"quoting": {"identifier": False}}

    @pytest.fixture(scope="class")
    def tests(self):
        return {"get_relation_test.sql": _TESTS__GET_RELATION_QUOTING}

    def test_simple_copy_quoting_off(self, project):
        super().test_simple_copy(project)
        run_dbt(["test"])

    @pytest.mark.skip(reason="Already run and test case above; no need to run again")
    def test_simple_copy(self):
        pass

    @pytest.mark.skip(reason="Already run and test case above; no need to run again")
    def test_simple_copy_with_materialized_views(self):
        pass


class TestEmptyModelsArentRun(EmptyModelsArentRunBase):
    pass


file_append = """
quoting:
    identifier: true
"""


class TestSimpleCopyBaseQuotingSwitch(SimpleCopySetup):
    @pytest.fixture(scope="class")
    def tests(self):
        return {"get_relation_test.sql": _TESTS__GET_RELATION_QUOTING}

    def test_seed_quoting_switch(self, project):
        results = run_dbt(["seed"])
        assert len(results) == 1

        # Update seed file
        main_seed_file = project.project_root / Path("seeds") / Path("seed.csv")
        rm_file(main_seed_file)
        write_file(_SEEDS__SEED_UPDATE, main_seed_file)

        # Change the profile temporarily
        dbt_project_yml = project.project_root / Path("dbt_project.yml")
        with open(dbt_project_yml, "r+") as f:
            dbt_project_yml_contents = f.read()
            f.write(file_append)
        run_dbt(["seed"], expect_pass=False)

        with open(dbt_project_yml, "w") as f:
            f.write(dbt_project_yml_contents)
        run_dbt(["test"])


inc_strat_yml = """
models:
    incremental_strategy: "delete+insert"
"""


class TestSnowflakeIncrementalOverwrite(SimpleCopySetup):
    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental_overwrite.sql": _MODELS__INCREMENTAL_OVERWRITE}

    def test__snowflake__incremental_overwrite(self, project):
        results = run_dbt(["seed"])
        assert len(results) == 1
        results = run_dbt(["run"])
        assert len(results) == 1

        # Fails using 'merge' strategy because there's a duplicate 'id'
        results = run_dbt(["run"], expect_pass=False)
        assert len(results) == 1

        # Setting the incremental_strategy should make this succeed
        dbt_project_yml = project.project_root / Path("dbt_project.yml")
        with open(dbt_project_yml, "a") as f:
            f.write(inc_strat_yml)

        results = run_dbt(["run"])
        assert len(results) == 1


class TestIncrementalMergeColumns(SimpleCopySetup):
    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental_update_cols.sql": _MODELS__INCREMENTAL_UPDATE_COLS}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "seed.csv": _SEEDS__SEED_MERGE_INITIAL,
            "expected_result.csv": _SEEDS__SEED_MERGE_EXPECTED,
        }

    def seed_and_run(self):
        run_dbt(["seed"])
        run_dbt(["run"])

    def test__snowflake__incremental_merge_columns(self, project):
        self.seed_and_run()

        main_seed_file = project.project_root / Path("seeds") / Path("seed.csv")
        rm_file(main_seed_file)
        write_file(_SEEDS__SEED_MERGE_UPDATE, main_seed_file)
        self.seed_and_run()
        check_relations_equal(project.adapter, ["incremental_update_cols", "expected_result"])
