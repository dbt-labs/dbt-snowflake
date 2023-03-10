import pytest
from dbt.tests.adapter.incremental.test_incremental_unique_id import BaseIncrementalUniqueKey


class TestUniqueKeySnowflake(BaseIncrementalUniqueKey):
    pass


class TestUniqueKeyDeleteInsertSnowflake(BaseIncrementalUniqueKey):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"+incremental_strategy": "delete+insert"}}
