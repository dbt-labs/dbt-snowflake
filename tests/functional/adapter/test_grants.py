import pytest
from dbt.tests.adapter.grants.test_incremental_grants import BaseIncrementalGrants
from dbt.tests.adapter.grants.test_invalid_grants import BaseInvalidGrants
from dbt.tests.adapter.grants.test_model_grants import BaseModelGrants
from dbt.tests.adapter.grants.test_seed_grants import BaseSeedGrants
from dbt.tests.adapter.grants.test_snapshot_grants import BaseSnapshotGrants

from dbt.context.base import BaseContext
from typing import Dict, List


class patch:
    # ideally this change would ripple up into dbt-adapter/dbt-core
    def assert_expected_grants_match_actual(self, project, relation_name, expected_grants):
        adapter = project.adapter
        actual_grants = self.get_grants_on_relation(project, relation_name)
        expected_grants_std = adapter.standardize_grant_config(expected_grants)

        # need a case-insensitive comparison -- this would not be true for all adapters
        # so just a simple "assert expected == actual_grants" won't work
        diff_a = adapter.diff_of_grants(actual_grants, expected_grants_std)
        diff_b = adapter.diff_of_grants(expected_grants_std, actual_grants)
        assert diff_a == diff_b == {}


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


class TestInvalidGrantsSnowflake(patch, BaseInvalidGrants):
    def grantee_does_not_exist_error(self):
        return "does not exist or not authorized"

    def privilege_does_not_exist_error(self):
        return "unexpected"


class TestModelGrantsSnowflake(patch, BaseModelGrants):
    pass


class TestModelGrantsCopyGrantsSnowflake(patch, BaseCopyGrantsSnowflake, BaseModelGrants):
    pass


class TestIncrementalGrantsSnowflake(patch, BaseIncrementalGrants):
    pass


class TestIncrementalCopyGrantsSnowflake(patch, BaseCopyGrantsSnowflake, BaseIncrementalGrants):
    pass


class TestSeedGrantsSnowflake(patch, BaseSeedGrants):
    pass


class TestSeedCopyGrantsSnowflake(patch, BaseCopyGrantsSnowflake, BaseSeedGrants):
    pass


class TestSnapshotGrants(patch, BaseSnapshotGrants):
    pass


class TestSnapshotCopyGrantsSnowflake(patch, BaseCopyGrantsSnowflake, BaseSnapshotGrants):
    pass
