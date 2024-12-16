import pytest
from .base_grants import BaseCopyGrantsSnowflake
from .base_incremental_grants import BaseIncrementalGrantsSnowflake


class BaseIncrementalGrantsSnowflakeDatabaseRole(BaseIncrementalGrantsSnowflake):
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
          database_role: ["test_database_role_1"]
""",
            "user2_incremental_model_schema_yml": """
version: 2
models:
  - name: my_incremental_model
    config:
      materialized: incremental
      grants:
        select:
          database_role: ["test_database_role_2"]
""",
        }


class TestIncrementalGrantsSnowflakeDatabaseRole(BaseIncrementalGrantsSnowflakeDatabaseRole):
    pass


# With "+copy_grants": True
class TestIncrementalGrantsCopyGrantsSnowflakeDatabaseRole(
    BaseCopyGrantsSnowflake, BaseIncrementalGrantsSnowflakeDatabaseRole
):
    pass
