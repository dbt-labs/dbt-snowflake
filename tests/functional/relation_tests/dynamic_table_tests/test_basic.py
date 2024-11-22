import pytest

from dbt.tests.util import run_dbt

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
            "my_dynamic_transient_table.sql": models.DYNAMIC_TRANSIENT_TABLE,
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
        assert (
            query_relation_type(project, "my_dynamic_transient_table") == "dynamic_table_transient"
        )
        if self.iceberg:
            assert query_relation_type(project, "my_dynamic_iceberg_table") == "dynamic_table"


class TestBasicIcebergOn(TestBasic):
    iceberg = True

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"enable_iceberg_materializations": True}}


class TestDefaultTransient(TestBasic):

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"default_dynamic_tables_to_transient": True}}

    def test_dynamic_table_full_refresh(self, project):
        run_dbt(["run", "--full-refresh"])
        assert (
            query_relation_type(project, "my_dynamic_transient_table") == "dynamic_table_transient"
        )
        assert query_relation_type(project, "my_dynamic_table_downstream") == "dynamic_table"
        assert query_relation_type(project, "my_dynamic_table") == "dynamic_table_transient"
