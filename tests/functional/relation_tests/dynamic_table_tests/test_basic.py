import pytest

from dbt.tests.util import run_dbt

from tests.functional.relation_tests.dynamic_table_tests import models
from tests.functional.utils import query_relation_type


class TestBasic:

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "my_dynamic_table.sql": models.DYNAMIC_TABLE,
            "my_dynamic_table_downstream.sql": models.DYNAMIC_TABLE_DOWNSTREAM,
            "my_dynamic_iceberg_table.sql": models.DYNAMIC_ICEBERG_TABLE,
        }

    @pytest.fixture(scope="class", autouse=True)
    def setup(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])

    def test_dynamic_table_full_refresh(self, project):
        run_dbt(["run", "--full-refresh"])
        assert query_relation_type(project, "my_dynamic_table") == "dynamic_table"
        assert query_relation_type(project, "my_dynamic_table_downstream") == "dynamic_table"
        assert query_relation_type(project, "my_dynamic_iceberg_table") == "dynamic_table"
