from .base_grants import BaseCopyGrantsSnowflake, BaseGrantsSnowflakePatch
from dbt.tests.adapter.grants.test_incremental_grants import BaseIncrementalGrants


# Run the adapter Snapshot Grant tests with patched assert_expected_grants_match_actual


class TestIncrementalGrantsSnowflake(BaseGrantsSnowflakePatch, BaseIncrementalGrants):
    pass


# With "+copy_grants": True
class TestIncrementalGrantsCopyGrantsSnowflake(
    BaseCopyGrantsSnowflake, BaseGrantsSnowflakePatch, BaseIncrementalGrants
):
    pass
