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
        }

    def test_run(self, project):
        run_dbt(["seed"])
        run_dbt(["run", "--empty"])
