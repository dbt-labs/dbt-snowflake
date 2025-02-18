import pytest
from .base_grants import BaseCopyGrantsSnowflake
from .base_seed_grants import BaseSeedGrantsSnowflake


class BaseSeedGrantsSnowflakeRole(BaseSeedGrantsSnowflake):
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
          role: ["{{ env_var('DBT_TEST_USER_1') }}"]
""",
            "user2_schema_base_yml": """
version: 2
seeds:
  - name: my_seed
    config:
      grants:
        select:
          role: ["{{ env_var('DBT_TEST_USER_2') }}"]
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
          role: []
""",
        }


class TestSeedGrantsSnowflakeRole(BaseSeedGrantsSnowflakeRole):
    pass


# With "+copy_grants": True
class TestSeedGrantsCopyGrantsSnowflakeRole(BaseCopyGrantsSnowflake, BaseSeedGrantsSnowflakeRole):
    pass
