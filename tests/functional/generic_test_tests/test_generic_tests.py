import pytest

from dbt.tests.util import run_dbt, run_dbt_and_capture

from tests.functional.generic_test_tests import _files


class TestWarehouseConfig:

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "colors.csv": _files.SEED__COLORS,
            "facts.csv": _files.SEED__FACTS,
        }

    @pytest.fixture(scope="class", autouse=True)
    def setup(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])
        yield


class TestWarehouseConfigControl(TestWarehouseConfig):

    @pytest.fixture(scope="class")
    def models(self):
        return {"schema.yml": _files.SCHEMA__CONTROL}

    def test_expected_warehouse(self, project):
        results, logs = run_dbt_and_capture(["test"])
        assert len(results) == 1


class TestWarehouseConfigExplicitWarehouse(TestWarehouseConfig):

    @pytest.fixture(scope="class")
    def models(self):
        return {"schema.yml": _files.SCHEMA__EXPLICIT_WAREHOUSE}

    def test_expected_warehouse(self, project):
        _, logs = run_dbt_and_capture(["test", "--log-level", "debug"])
        assert "use warehouse " in logs


class TestWarehouseConfigNotNull(TestWarehouseConfig):

    @pytest.fixture(scope="class")
    def models(self):
        return {"schema.yml": _files.SCHEMA__NOT_NULL}

    def test_expected_warehouse(self, project):
        _, logs = run_dbt_and_capture(["test", "--log-level", "debug"], expect_pass=False)
        assert "use warehouse " in logs
