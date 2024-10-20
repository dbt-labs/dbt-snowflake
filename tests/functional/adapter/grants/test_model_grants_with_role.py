import pytest
from .base_grants import BaseCopyGrantsSnowflake
from .base_model_grants import BaseModelGrantsSnowflake


class BaseModelGrantsSnowflakeRole(BaseModelGrantsSnowflake):
    """
    The base adapter model grant test but using new role syntax
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
          role: ["{{ env_var('DBT_TEST_USER_1') }}"]
""",
            "user2_model_schema_yml": """
version: 2
models:
  - name: my_model
    config:
      grants:
        select:
          role: ["{{ env_var('DBT_TEST_USER_2') }}"]
""",
            "table_model_schema_yml": """
version: 2
models:
  - name: my_model
    config:
      materialized: table
      grants:
        select:
          role: ["{{ env_var('DBT_TEST_USER_1') }}"]
""",
            "user2_table_model_schema_yml": """
version: 2
models:
  - name: my_model
    config:
      materialized: table
      grants:
        select:
          role: ["{{ env_var('DBT_TEST_USER_2') }}"]
""",
            "multiple_users_table_model_schema_yml": """
version: 2
models:
  - name: my_model
    config:
      materialized: table
      grants:
        select:
          role: ["{{ env_var('DBT_TEST_USER_1') }}", "{{ env_var('DBT_TEST_USER_2') }}"]
""",
            "multiple_privileges_table_model_schema_yml": """
version: 2
models:
  - name: my_model
    config:
      materialized: table
      grants:
        select:
          role: ["{{ env_var('DBT_TEST_USER_1') }}"]
        insert:
          role: ["{{ env_var('DBT_TEST_USER_2') }}"]
""",
        }


class TestModelGrantsSnowflakeRole(BaseModelGrantsSnowflakeRole):
    pass


# With "+copy_grants": True
class TestModelGrantsCopyGrantsSnowflakeRole(
    BaseCopyGrantsSnowflake, BaseModelGrantsSnowflakeRole
):
    pass
