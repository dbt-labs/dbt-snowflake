import pytest
from .base_grants import BaseCopyGrantsSnowflake
from .base_invalid_grants import BaseInvalidGrantsSnowflake


class BaseInvalidGrantsSnowflakeDatabaseRole(BaseInvalidGrantsSnowflake):
    """
    The base adapter Invalid grant test but using database roles
    """

    @pytest.fixture(scope="class")
    def schema_yml(self, prefix):
        return {
            "invalid_user_table_model_schema_yml": """
version: 2
models:
  - name: my_invalid_model
    config:
      materialized: table
      grants:
        select:
            database_role: ['invalid_user']
""",
            "invalid_privilege_table_model_schema_yml": """
version: 2
models:
  - name: my_invalid_model
    config:
      materialized: table
      grants:
        fake_privilege:
            database_role: ["test_database_role_1"]
""",
        }


class TestModelGrantsSnowflakeDatabaseRole(BaseInvalidGrantsSnowflakeDatabaseRole):
    pass


# With "+copy_grants": True
class TestModelGrantsCopyGrantsSnowflakeDatabaseRole(
    BaseCopyGrantsSnowflake, BaseInvalidGrantsSnowflakeDatabaseRole
):
    pass
