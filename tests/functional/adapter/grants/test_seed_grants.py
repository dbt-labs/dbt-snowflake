from .base_grants import BaseCopyGrantsSnowflake, BaseGrantsSnowflakePatch
from dbt.tests.adapter.grants.test_seed_grants import BaseSeedGrants


# Run the adapter Seed Grant tests with patched assert_expected_grants_match_actual


class TestSeedGrantsSnowflake(BaseGrantsSnowflakePatch, BaseSeedGrants):
    pass


# With "+copy_grants": True
class TestSeedGrantsCopyGrantsSnowflake(
    BaseCopyGrantsSnowflake, BaseGrantsSnowflakePatch, BaseSeedGrants
):
    pass
