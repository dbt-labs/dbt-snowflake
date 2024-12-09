import pytest

from dbt.tests.util import assert_message_in_logs, run_dbt, run_dbt_and_capture

from tests.functional.relation_tests.dynamic_table_tests import models
from tests.functional.utils import query_relation_type


class TestBasic:
    iceberg: bool = False

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        my_models = {
            "my_dynamic_table.sql": models.DYNAMIC_TABLE,
            "my_dynamic_table_downstream.sql": models.DYNAMIC_TABLE_DOWNSTREAM,
        }
        if self.iceberg:
            my_models.update(
                {
                    "my_dynamic_iceberg_table.sql": models.DYNAMIC_ICEBERG_TABLE,
                }
            )
        yield my_models

    @pytest.fixture(scope="class", autouse=True)
    def setup(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])

    def test_dynamic_table_full_refresh(self, project):
        run_dbt(["run", "--full-refresh"])
        assert query_relation_type(project, "my_dynamic_table") == "dynamic_table"
        assert query_relation_type(project, "my_dynamic_table_downstream") == "dynamic_table"
        if self.iceberg:
            assert query_relation_type(project, "my_dynamic_iceberg_table") == "dynamic_table"


class TestBasicIcebergOn(TestBasic):
    iceberg = True

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"enable_iceberg_materializations": True}}


class TestAutoConfigDoesntRebuild:
    DT_NAME = "my_dynamic_table"

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            f"{self.DT_NAME}.sql": models.AUTO_DYNAMIC_TABLE,
        }

    def test_auto_config_doesnt_rebuild(self, project):
        model_qualified_name = f"{project.database}.{project.test_schema}.{self.DT_NAME}"

        run_dbt(["seed"])
        _, logs = run_dbt_and_capture(["--debug", "run", "--select", f"{self.DT_NAME}.sql"])
        assert_message_in_logs(f"create dynamic table {model_qualified_name}", logs)
        assert_message_in_logs("refresh_mode = AUTO", logs)

        _, logs = run_dbt_and_capture(["--debug", "run", "--select", f"{self.DT_NAME}.sql"])
        assert_message_in_logs(f"create dynamic table {model_qualified_name}", logs, False)
        assert_message_in_logs("refresh_mode = AUTO", logs, False)
        assert_message_in_logs(
            f"No configuration changes were identified on on: `{model_qualified_name}`. Continuing.",
            logs,
        )
