import pytest

from .base_grants import BaseGrantsSnowflake
from dbt.tests.util import (
    get_manifest,
    run_dbt,
    run_dbt_and_capture,
    write_file,
)

seeds__my_seed_csv = """
id,name,some_date
1,Easton,1981-05-20T06:46:51
2,Lillian,1978-09-03T18:10:33
""".lstrip()

schema_yml = {
    "schema_base_yml": """
version: 2
seeds:
  - name: my_seed
    config:
      grants:
        select: ["{{ env_var('DBT_TEST_USER_1') }}"]
""",
    "user2_schema_base_yml": """
version: 2
seeds:
  - name: my_seed
    config:
      grants:
        select: ["{{ env_var('DBT_TEST_USER_2') }}"]
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
        select: []
""",
}


class BaseSeedGrantsSnowflake(BaseGrantsSnowflake):
    def seeds_support_partial_refresh(self):
        return True

    @pytest.fixture(scope="class")
    def schema_yml(self):
        return schema_yml

    @pytest.fixture(scope="class")
    def seeds(self, schema_yml):
        updated_schema = self.interpolate_name_overrides(schema_yml["schema_base_yml"])
        return {
            "my_seed.csv": seeds__my_seed_csv,
            "schema.yml": updated_schema,
        }

    def get_seed(self, seed_id, project):
        manifest = get_manifest(project.project_root)
        return manifest.nodes[seed_id]

    def test_seed_grants(self, project, get_test_users, schema_yml):
        test_users = get_test_users
        select_privilege_name = self.privilege_grantee_name_overrides()["select"]

        # seed command
        (results, log_output) = run_dbt_and_capture(["--debug", "seed"])
        assert len(results) == 1
        seed = self.get_seed("seed.test.my_seed", project)
        assert "grant " in log_output
        self.assert_expected_grants_match_actual(project, "my_seed", seed.config.grants)

        # run it again, with no config changes
        (results, log_output) = run_dbt_and_capture(["--debug", "seed"])
        assert len(results) == 1
        if self.seeds_support_partial_refresh():
            # grants carried over -- nothing should have changed
            assert "revoke " not in log_output
            assert "grant " not in log_output
        else:
            # seeds are always full-refreshed on this adapter, so we need to re-grant
            assert "grant " in log_output

        seed = self.get_seed("seed.test.my_seed", project)
        self.assert_expected_grants_match_actual(project, "my_seed", seed.config.grants)

        # change the grantee, assert it updates
        updated_yaml = self.interpolate_name_overrides(schema_yml["user2_schema_base_yml"])
        write_file(updated_yaml, project.project_root, "seeds", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "seed"])
        assert len(results) == 1
        seed = self.get_seed("seed.test.my_seed", project)
        self.assert_expected_grants_match_actual(project, "my_seed", seed.config.grants)

        # run it again, with --full-refresh, grants should be the same
        run_dbt(["seed", "--full-refresh"])
        seed = self.get_seed("seed.test.my_seed", project)
        self.assert_expected_grants_match_actual(project, "my_seed", seed.config.grants)

        previous_grants = seed.config.grants

        # change config to 'grants: {}' -- should be completely ignored
        updated_yaml = self.interpolate_name_overrides(schema_yml["ignore_grants_yml"])
        write_file(updated_yaml, project.project_root, "seeds", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "seed"])
        assert len(results) == 1
        assert "revoke " not in log_output
        assert "grant " not in log_output
        seed = self.get_seed("seed.test.my_seed", project)
        expected_config = {}
        expected_actual = previous_grants
        assert seed.config.grants == expected_config
        if self.seeds_support_partial_refresh():
            # ACTUAL grants will NOT match expected grants
            self.assert_expected_grants_match_actual(project, "my_seed", expected_actual)
        else:
            # there should be ZERO grants on the seed
            self.assert_expected_grants_match_actual(project, "my_seed", expected_config)

        # now run with ZERO grants -- all grants should be removed
        # whether explicitly (revoke) or implicitly (recreated without any grants added on)
        updated_yaml = self.interpolate_name_overrides(schema_yml["zero_grants_yml"])
        write_file(updated_yaml, project.project_root, "seeds", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "seed"])
        assert len(results) == 1
        if self.seeds_support_partial_refresh():
            assert "revoke " in log_output
        expected = {}
        seed = self.get_seed("seed.test.my_seed", project)
        self.assert_expected_grants_match_actual(project, "my_seed", seed.config.grants)

        # run it again -- dbt shouldn't try to grant or revoke anything
        (results, log_output) = run_dbt_and_capture(["--debug", "seed"])
        assert len(results) == 1
        assert "revoke " not in log_output
        assert "grant " not in log_output
        seed = self.get_seed("seed.test.my_seed", project)
        self.assert_expected_grants_match_actual(project, "my_seed", seed.config.grants)
