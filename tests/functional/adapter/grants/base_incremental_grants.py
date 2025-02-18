import pytest

from .base_grants import BaseGrantsSnowflake
from dbt.tests.util import (
    get_connection,
    get_manifest,
    relation_from_name,
    run_dbt,
    run_dbt_and_capture,
    write_file,
)


my_incremental_model_sql = """
  select 1 as fun
"""

schema_yml = {
    "incremental_model_schema_yml": """
version: 2
models:
  - name: my_incremental_model
    config:
      materialized: incremental
      grants:
        select: ["{{ env_var('DBT_TEST_USER_1') }}"]
""",
    "user2_incremental_model_schema_yml": """
version: 2
models:
  - name: my_incremental_model
    config:
      materialized: incremental
      grants:
        select: ["{{ env_var('DBT_TEST_USER_2') }}"]
""",
}


class BaseIncrementalGrantsSnowflake(BaseGrantsSnowflake):
    @pytest.fixture(scope="class")
    def schema_yml(self):
        return schema_yml

    @pytest.fixture(scope="class")
    def models(self, schema_yml):
        updated_schema = self.interpolate_name_overrides(
            schema_yml["incremental_model_schema_yml"]
        )
        return {
            "my_incremental_model.sql": my_incremental_model_sql,
            "schema.yml": updated_schema,
        }

    def get_model(self, model_id, project):
        manifest = get_manifest(project.project_root)
        return manifest.nodes[model_id]

    def test_incremental_grants(self, project, get_test_users, schema_yml):
        # we want the test to fail, not silently skip
        test_users = get_test_users
        select_privilege_name = self.privilege_grantee_name_overrides()["select"]
        assert len(test_users) == 3

        # Incremental materialization, single select grant
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        model = self.get_model("model.test.my_incremental_model", project)
        assert model.config.materialized == "incremental"
        self.assert_expected_grants_match_actual(
            project, "my_incremental_model", model.config.grants
        )

        # Incremental materialization, run again without changes
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        assert "revoke " not in log_output
        assert "grant " not in log_output  # with space to disambiguate from 'show grants'
        model = self.get_model("model.test.my_incremental_model", project)
        self.assert_expected_grants_match_actual(
            project, "my_incremental_model", model.config.grants
        )

        # Incremental materialization, change select grant user
        updated_yaml = self.interpolate_name_overrides(
            schema_yml["user2_incremental_model_schema_yml"]
        )
        write_file(updated_yaml, project.project_root, "models", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        assert "revoke " in log_output
        model = self.get_model("model.test.my_incremental_model", project)
        assert model.config.materialized == "incremental"
        self.assert_expected_grants_match_actual(
            project, "my_incremental_model", model.config.grants
        )

        # Incremental materialization, same config, now with --full-refresh
        run_dbt(["--debug", "run", "--full-refresh"])
        assert len(results) == 1
        model = self.get_model("model.test.my_incremental_model", project)
        # whether grants or revokes happened will vary by adapter
        self.assert_expected_grants_match_actual(
            project, "my_incremental_model", model.config.grants
        )

        # Now drop the schema (with the table in it)
        adapter = project.adapter
        relation = relation_from_name(adapter, "my_incremental_model")
        with get_connection(adapter):
            adapter.drop_schema(relation)

        # Incremental materialization, same config, rebuild now that table is missing
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        assert "grant " in log_output
        assert "revoke " not in log_output
        model = self.get_model("model.test.my_incremental_model", project)
        self.assert_expected_grants_match_actual(
            project, "my_incremental_model", model.config.grants
        )
