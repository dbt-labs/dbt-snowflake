import pytest
from .base_grants import BaseCopyGrantsSnowflake
from .base_snapshot_grants import BaseSnapshotGrantsSnowflake


class BaseSnapshotGrantsSnowflakeDatabaseRole(BaseSnapshotGrantsSnowflake):
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
          database_role: ["test_database_role_1"]
""",
            "user2_snapshot_schema_yml": """
version: 2
snapshots:
  - name: my_snapshot
    config:
      grants:
        select:
          database_role: ["test_database_role_2"]
""",
        }


class TestSnapshotGrantsSnowflakeDatabaseRole(BaseSnapshotGrantsSnowflakeDatabaseRole):
    pass


# With "+copy_grants": True
class TestSnapshotGrantsCopyGrantsSnowflakeDatabaseRole(
    BaseCopyGrantsSnowflake, BaseSnapshotGrantsSnowflakeDatabaseRole
):
    pass
