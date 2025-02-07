import pytest

from .base_grants import BaseGrantsSnowflake
from dbt.tests.util import (
    get_manifest,
    run_dbt_and_capture,
    write_file,
)


my_model_sql = """
  select 1 as fun
"""

schema_yml = {
    "model_schema_yml": """
version: 2
models:
  - name: my_model
    config:
      grants:
        select: ["{{ env_var('DBT_TEST_USER_1') }}"]
""",
    "user2_model_schema_yml": """
version: 2
models:
  - name: my_model
    config:
      grants:
        select: ["{{ env_var('DBT_TEST_USER_2') }}"]
""",
    "table_model_schema_yml": """
version: 2
models:
  - name: my_model
    config:
      materialized: table
      grants:
        select: ["{{ env_var('DBT_TEST_USER_1') }}"]
""",
    "user2_table_model_schema_yml": """
version: 2
models:
  - name: my_model
    config:
      materialized: table
      grants:
        select: ["{{ env_var('DBT_TEST_USER_2') }}"]
""",
    "multiple_users_table_model_schema_yml": """
version: 2
models:
  - name: my_model
    config:
      materialized: table
      grants:
        select: ["{{ env_var('DBT_TEST_USER_1') }}", "{{ env_var('DBT_TEST_USER_2') }}"]
""",
    "multiple_privileges_table_model_schema_yml": """
version: 2
models:
  - name: my_model
    config:
      materialized: table
      grants:
        select: ["{{ env_var('DBT_TEST_USER_1') }}"]
        insert: ["{{ env_var('DBT_TEST_USER_2') }}"]
""",
}


# Create our own copy of BaseModelGrants that allows is to overide the model_schemas
class BaseModelGrantsSnowflake(BaseGrantsSnowflake):
    @pytest.fixture(scope="class")
    def schema_yml(self):
        return schema_yml

    @pytest.fixture(scope="class")
    def models(self, schema_yml):
        updated_schema = self.interpolate_name_overrides(schema_yml["model_schema_yml"])
        return {
            "my_model.sql": my_model_sql,
            "schema.yml": updated_schema,
        }

    def get_model(self, model_id, project):
        manifest = get_manifest(project.project_root)
        return manifest.nodes[model_id]

    def test_view_table_grants(self, project, get_test_users, schema_yml):
        # we want the test to fail, not silently skip
        test_users = get_test_users
        select_privilege_name = self.privilege_grantee_name_overrides()["select"]
        insert_privilege_name = self.privilege_grantee_name_overrides()["insert"]
        assert len(test_users) == 3

        # View materialization, single select grant
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        model = self.get_model("model.test.my_model", project)
        assert model.config.materialized == "view"
        self.assert_expected_grants_match_actual(project, "my_model", model.config.grants)

        # View materialization, change select grant user
        updated_yaml = self.interpolate_name_overrides(schema_yml["user2_model_schema_yml"])
        write_file(updated_yaml, project.project_root, "models", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        model = self.get_model("model.test.my_model", project)
        self.assert_expected_grants_match_actual(project, "my_model", model.config.grants)

        # Table materialization, single select grant
        updated_yaml = self.interpolate_name_overrides(schema_yml["table_model_schema_yml"])
        write_file(updated_yaml, project.project_root, "models", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        model = self.get_model("model.test.my_model", project)
        assert model.config.materialized == "table"
        self.assert_expected_grants_match_actual(project, "my_model", model.config.grants)

        # Table materialization, change select grant user
        updated_yaml = self.interpolate_name_overrides(schema_yml["user2_table_model_schema_yml"])
        write_file(updated_yaml, project.project_root, "models", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        model = self.get_model("model.test.my_model", project)
        self.assert_expected_grants_match_actual(project, "my_model", model.config.grants)

        # Table materialization, multiple grantees
        updated_yaml = self.interpolate_name_overrides(
            schema_yml["multiple_users_table_model_schema_yml"]
        )
        write_file(updated_yaml, project.project_root, "models", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        model = self.get_model("model.test.my_model", project)
        assert model.config.materialized == "table"
        self.assert_expected_grants_match_actual(project, "my_model", model.config.grants)

        # Table materialization, multiple privileges
        updated_yaml = self.interpolate_name_overrides(
            schema_yml["multiple_privileges_table_model_schema_yml"]
        )
        write_file(updated_yaml, project.project_root, "models", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        model = self.get_model("model.test.my_model", project)
        assert model.config.materialized == "table"
        self.assert_expected_grants_match_actual(project, "my_model", model.config.grants)
