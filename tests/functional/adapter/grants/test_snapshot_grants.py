from .base_grants import BaseCopyGrantsSnowflake, BaseGrantsSnowflakePatch
from dbt.tests.adapter.grants.test_snapshot_grants import BaseSnapshotGrants


# Run the adapter Snapshot Grant tests with patched assert_expected_grants_match_actual


class TestSnapshotGrantsSnowflake(BaseGrantsSnowflakePatch, BaseSnapshotGrants):
    pass


# With "+copy_grants": True
class TestSnapshotGrantsCopyGrantsSnowflake(
    BaseCopyGrantsSnowflake, BaseGrantsSnowflakePatch, BaseSnapshotGrants
):
    pass
