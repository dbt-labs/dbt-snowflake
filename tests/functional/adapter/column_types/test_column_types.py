import pytest
from dbt.tests.adapter.column_types.test_column_types import BaseColumnTypes
from tests.functional.adapter.column_types.fixtures import _MODEL_SQL, _SCHEMA_YML


class TestSnowflakeColumnTypes(BaseColumnTypes):
    @pytest.fixture(scope="class")
    def models(self):
        return {"model.sql": _MODEL_SQL, "schema.yml": _SCHEMA_YML}

    def test_run_and_test(self, project):
        self.run_and_test()
