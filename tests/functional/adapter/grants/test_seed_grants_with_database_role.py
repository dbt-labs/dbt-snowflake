import pytest
from .base_grants import BaseCopyGrantsSnowflake
from .base_seed_grants import BaseSeedGrantsSnowflake


class BaseSeedGrantsSnowflakeDatabaseRole(BaseSeedGrantsSnowflake):
    """
    The base adapter model grant test but using new role syntax
    """

    @pytest.fixture(scope="class")
    def schema_yml(self, prefix):
        return {
            "schema_base_yml": """
version: 2
seeds:
  - name: my_seed
    config:
      grants:
        select:
          database_role: ["test_database_role_1"]
""",
            "user2_schema_base_yml": """
version: 2
seeds:
  - name: my_seed
    config:
      grants:
        select:
          database_role: ["test_database_role_2"]
""",
            "ignore_grants_yml": """
version: 2
seeds:
  - name: my_seed
    config:
      grants: {}
""",
            "zero_grants_yml": """
version: 2
seeds:
  - name: my_seed
    config:
      grants:
        select:
          database_role: []
""",
        }


class TestSeedGrantsSnowflakeDatabaseRole(BaseSeedGrantsSnowflakeDatabaseRole):
    pass


# With "+copy_grants": True
class TestSeedGrantsCopyGrantsSnowflakeDatabaseRole(
    BaseCopyGrantsSnowflake, BaseSeedGrantsSnowflakeDatabaseRole
):
    pass
