from .base_grants import BaseCopyGrantsSnowflake, BaseGrantsSnowflakePatch
from dbt.tests.adapter.grants.test_model_grants import BaseModelGrants


# Run the adapter Model Grant tests with patched assert_expected_grants_match_actual


class TestModelGrantsSnowflake(BaseGrantsSnowflakePatch, BaseModelGrants):
    pass


# With "+copy_grants": True
class TestModelGrantsCopyGrantsSnowflake(
    BaseCopyGrantsSnowflake, BaseGrantsSnowflakePatch, BaseModelGrants
):
    pass
