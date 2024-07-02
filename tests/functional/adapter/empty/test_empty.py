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
            "schema.yml": _models.SCHEMA,
            "control.sql": _models.CONTROL,
            "get_columns_in_relation.sql": _models.GET_COLUMNS_IN_RELATION,
            "alter_column_type.sql": _models.ALTER_COLUMN_TYPE,
            "alter_relation_comment.sql": _models.ALTER_RELATION_COMMENT,
            "alter_column_comment.sql": _models.ALTER_COLUMN_COMMENT,
            "alter_relation_add_remove_columns.sql": _models.ALTER_RELATION_ADD_REMOVE_COLUMNS,
            "truncate_relation.sql": _models.TRUNCATE_RELATION,
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
            "alter_relation_comment",
            "alter_column_comment",
            "alter_relation_add_remove_columns",
            "truncate_relation",
        ],
    )
    def test_run(self, project, model):
        run_dbt(["run", "--empty", "--select", model])
