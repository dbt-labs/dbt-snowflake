import pytest
from .base_grants import BaseCopyGrantsSnowflake
from .base_model_grants import BaseModelGrantsSnowflake


class BaseModelGrantsSnowflakeDatabaseRole(BaseModelGrantsSnowflake):
    """
    The base adapter model grant test but using database roles
    """

    @pytest.fixture(scope="class")
    def schema_yml(self, prefix):
        return {
            "model_schema_yml": """
version: 2
models:
  - name: my_model
    config:
      grants:
        select:
          database_role: ["test_database_role_1"]
""",
            "user2_model_schema_yml": """
version: 2
models:
  - name: my_model
    config:
      grants:
        select:
          database_role: ["test_database_role_2"]
""",
            "table_model_schema_yml": """
version: 2
models:
  - name: my_model
    config:
      materialized: table
      grants:
        select:
          database_role: ["test_database_role_1"]
""",
            "user2_table_model_schema_yml": """
version: 2
models:
  - name: my_model
    config:
      materialized: table
      grants:
        select:
          database_role: ["test_database_role_2"]
""",
            "multiple_users_table_model_schema_yml": """
version: 2
models:
  - name: my_model
    config:
      materialized: table
      grants:
        select:
          database_role: ["test_database_role_1", "test_database_role_2"]
""",
            "multiple_privileges_table_model_schema_yml": """
version: 2
models:
  - name: my_model
    config:
      materialized: table
      grants:
        select:
          database_role: ["test_database_role_1"]
        insert:
          database_role: ["test_database_role_2"]
""",
        }


class TestModelGrantsSnowflakeDatabaseRole(BaseModelGrantsSnowflakeDatabaseRole):
    pass


# With "+copy_grants": True
class TestModelGrantsCopyGrantsSnowflakeDatabaseRole(
    BaseCopyGrantsSnowflake, BaseModelGrantsSnowflakeDatabaseRole
):
    pass
