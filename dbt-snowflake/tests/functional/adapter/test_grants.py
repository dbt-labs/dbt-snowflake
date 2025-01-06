import pytest
from dbt.tests.adapter.grants.test_incremental_grants import BaseIncrementalGrants
from dbt.tests.adapter.grants.test_invalid_grants import BaseInvalidGrants
from dbt.tests.adapter.grants.test_model_grants import BaseModelGrants
from dbt.tests.adapter.grants.test_seed_grants import BaseSeedGrants
from dbt.tests.adapter.grants.test_snapshot_grants import BaseSnapshotGrants


class BaseCopyGrantsSnowflake:
    # Try every test case without copy_grants enabled (default),
    # and with copy_grants enabled (this base class)
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+copy_grants": True,
            },
            "seeds": {
                "+copy_grants": True,
            },
            "snapshots": {
                "+copy_grants": True,
            },
        }


class TestInvalidGrantsSnowflake(BaseInvalidGrants):
    def grantee_does_not_exist_error(self):
        return "does not exist or not authorized"

    def privilege_does_not_exist_error(self):
        return "unexpected"


class TestModelGrantsSnowflake(BaseModelGrants):
    pass


class TestModelGrantsCopyGrantsSnowflake(BaseCopyGrantsSnowflake, BaseModelGrants):
    pass


class TestIncrementalGrantsSnowflake(BaseIncrementalGrants):
    pass


class TestIncrementalCopyGrantsSnowflake(BaseCopyGrantsSnowflake, BaseIncrementalGrants):
    pass


class TestSeedGrantsSnowflake(BaseSeedGrants):
    pass


class TestSeedCopyGrantsSnowflake(BaseCopyGrantsSnowflake, BaseSeedGrants):
    pass


class TestSnapshotGrants(BaseSnapshotGrants):
    pass


class TestSnapshotCopyGrantsSnowflake(BaseCopyGrantsSnowflake, BaseSnapshotGrants):
    pass
