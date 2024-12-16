import pytest
from .base_grants import BaseCopyGrantsSnowflake
from .base_incremental_grants import BaseIncrementalGrantsSnowflake


class BaseIncrementalGrantsSnowflakeRole(BaseIncrementalGrantsSnowflake):
    """
    The base adapter model grant test but using new role syntax
    """

    @pytest.fixture(scope="class")
    def schema_yml(self, prefix):
        return {
            "incremental_model_schema_yml": """
version: 2
models:
  - name: my_incremental_model
    config:
      materialized: incremental
      grants:
        select:
          role: ["{{ env_var('DBT_TEST_USER_1') }}"]
""",
            "user2_incremental_model_schema_yml": """
version: 2
models:
  - name: my_incremental_model
    config:
      materialized: incremental
      grants:
        select:
          role: ["{{ env_var('DBT_TEST_USER_2') }}"]
""",
        }


class TestIncrementalGrantsSnowflakeRole(BaseIncrementalGrantsSnowflakeRole):
    pass


# With "+copy_grants": True
class TestIncrementalGrantsCopyGrantsSnowflakeRole(
    BaseCopyGrantsSnowflake, BaseIncrementalGrantsSnowflakeRole
):
    pass
