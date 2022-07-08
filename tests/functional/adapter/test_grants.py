from dbt.tests.adapter.grants.base_grants import BaseGrants
from dbt.tests.adapter.grants.test_incremental_grants import BaseIncrementalGrants
from dbt.tests.adapter.grants.test_invalid_grants import BaseInvalidGrants
from dbt.tests.adapter.grants.test_model_grants import BaseModelGrants
# to do: from dbt.tests.adapter.grants.test_revoke_all import
from dbt.tests.adapter.grants.test_seed_grants import BaseSeedGrants
from dbt.tests.adapter.grants.test_snapshot_grants import BaseSnapshotGrants


class TestGrantsSnowflake(BaseGrants):
    pass


class TestIncrementalGrantsSnowflake(BaseIncrementalGrants):
    pass


class TestInvalidGrantsSnowflake(BaseInvalidGrants):
    def grantee_does_not_exist_error(self):
        return "does not exist or not authorized"

    def privilege_does_not_exist_error(self):
        return "unexpected"


class TestModelGrantsSnowflake(BaseModelGrants):
    pass


class TestSeedGrantsSnowflake(BaseSeedGrants):
    pass


class TestSnapshotGrants(BaseSnapshotGrants):
    pass