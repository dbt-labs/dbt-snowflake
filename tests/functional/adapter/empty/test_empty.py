from dbt.tests.adapter.empty.test_empty import BaseTestEmpty, BaseTestEmptyInlineSourceRef
from dbt.tests.util import run_dbt
import pytest

from tests.functional.adapter.empty import _models


class TestSnowflakeEmpty(BaseTestEmpty):
    pass


class TestSnowflakeEmptyInlineSourceRef(BaseTestEmptyInlineSourceRef):
    pass


class TestMetadataWithEmptyFlag:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"my_seed.csv": _models.SEED}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "control.sql": _models.CONTROL,
            "get_columns_in_relation.sql": _models.GET_COLUMNS_IN_RELATION,
            "alter_column_type.sql": _models.ALTER_COLUMN_TYPE,
        }

    @pytest.fixture(scope="class", autouse=True)
    def setup(self, project):
        run_dbt(["seed"])

    @pytest.mark.parametrize(
        "model",
        [
            "control",
            "get_columns_in_relation",
            "alter_column_type",
        ],
    )
    def test_run(self, project, model):
        run_dbt(["run", "--empty", "--select", model])
