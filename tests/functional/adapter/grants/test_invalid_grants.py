from .base_grants import BaseCopyGrantsSnowflake, BaseGrantsSnowflakePatch
from dbt.tests.adapter.grants.test_invalid_grants import BaseInvalidGrants


# Run the adapter Snapshot Grant tests with patched assert_expected_grants_match_actual


class InvalidGrantsSnowflake(BaseInvalidGrants):
    def grantee_does_not_exist_error(self):
        return "does not exist or not authorized"

    def privilege_does_not_exist_error(self):
        return "unexpected"


class TestInvaildGrantsSnowflake(BaseGrantsSnowflakePatch, InvalidGrantsSnowflake):
    pass


# With "+copy_grants": True
class TestInvalidGrantsCopyGrantsSnowflake(
    BaseCopyGrantsSnowflake, BaseGrantsSnowflakePatch, InvalidGrantsSnowflake
):
    pass
