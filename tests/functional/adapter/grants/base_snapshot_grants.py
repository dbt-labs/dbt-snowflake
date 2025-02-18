import pytest
from .base_grants import BaseGrantsSnowflake
from dbt.tests.util import (
    get_manifest,
    run_dbt,
    run_dbt_and_capture,
    write_file,
)


my_snapshot_sql = """
{% snapshot my_snapshot %}
    {{ config(
        check_cols='all', unique_key='id', strategy='check',
        target_database=database, target_schema=schema
    ) }}
    select 1 as id, cast('blue' as {{ type_string() }}) as color
{% endsnapshot %}
""".strip()

schema_yml = {
    "snapshot_schema_yml": """
version: 2
snapshots:
  - name: my_snapshot
    config:
      grants:
        select: ["{{ env_var('DBT_TEST_USER_1') }}"]
""",
    "user2_snapshot_schema_yml": """
version: 2
snapshots:
  - name: my_snapshot
    config:
      grants:
        select: ["{{ env_var('DBT_TEST_USER_2') }}"]
""",
}


class BaseSnapshotGrantsSnowflake(BaseGrantsSnowflake):
    @pytest.fixture(scope="class")
    def schema_yml(self):
        return schema_yml

    @pytest.fixture(scope="class")
    def snapshots(self, schema_yml):
        return {
            "my_snapshot.sql": my_snapshot_sql,
            "schema.yml": self.interpolate_name_overrides(schema_yml["snapshot_schema_yml"]),
        }

    def get_snapshot(self, snapshot_id, project):
        manifest = get_manifest(project.project_root)
        return manifest.nodes[snapshot_id]

    def test_snapshot_grants(self, project, get_test_users, schema_yml):
        test_users = get_test_users
        select_privilege_name = self.privilege_grantee_name_overrides()["select"]

        # run the snapshot
        results = run_dbt(["snapshot"])
        assert len(results) == 1
        snapshot = self.get_snapshot("snapshot.test.my_snapshot", project)
        self.assert_expected_grants_match_actual(project, "my_snapshot", snapshot.config.grants)

        # run it again, nothing should have changed
        (results, log_output) = run_dbt_and_capture(["--debug", "snapshot"])
        assert len(results) == 1
        snapshot = self.get_snapshot("snapshot.test.my_snapshot", project)
        assert "revoke " not in log_output
        assert "grant " not in log_output
        self.assert_expected_grants_match_actual(project, "my_snapshot", snapshot.config.grants)

        # change the grantee, assert it updates
        updated_yaml = self.interpolate_name_overrides(schema_yml["user2_snapshot_schema_yml"])
        write_file(updated_yaml, project.project_root, "snapshots", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "snapshot"])
        assert len(results) == 1
        snapshot = self.get_snapshot("snapshot.test.my_snapshot", project)
        self.assert_expected_grants_match_actual(project, "my_snapshot", snapshot.config.grants)
