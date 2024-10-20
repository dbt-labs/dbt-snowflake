import pytest
from .base_grants import BaseCopyGrantsSnowflake
from .base_snapshot_grants import BaseSnapshotGrantsSnowflake


class BaseSnapshotGrantsSnowflakeRole(BaseSnapshotGrantsSnowflake):
    """
    The base adapter Snapshot grant test but using new role syntax
    """

    @pytest.fixture(scope="class")
    def schema_yml(self, prefix):
        return {
            "snapshot_schema_yml": """
version: 2
snapshots:
  - name: my_snapshot
    config:
      grants:
        select:
          role: ["{{ env_var('DBT_TEST_USER_1') }}"]
""",
            "user2_snapshot_schema_yml": """
version: 2
snapshots:
  - name: my_snapshot
    config:
      grants:
        select:
          role: ["{{ env_var('DBT_TEST_USER_2') }}"]
""",
        }


class TestSnapshotGrantsSnowflakeRole(BaseSnapshotGrantsSnowflakeRole):
    pass


# With "+copy_grants": True
class TestSnapshotGrantsCopyGrantsSnowflakeRole(
    BaseCopyGrantsSnowflake, BaseSnapshotGrantsSnowflakeRole
):
    pass
