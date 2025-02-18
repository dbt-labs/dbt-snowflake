import os
import pytest
import snowflake.connector
from dbt.tests.adapter.grants.base_grants import BaseGrants as OrigBaseGrants


class BaseGrantsSnowflakePatch:
    """
    Overides the adapter BaseGrants to use new adapter functions
    for grants.
    """

    def assert_expected_grants_match_actual(self, project, relation_name, expected_grants):
        adapter = project.adapter
        actual_grants = self.get_grants_on_relation(project, relation_name)
        expected_grants_std = adapter.standardize_grant_config(expected_grants)

        # need a case-insensitive comparison -- this would not be true for all adapters
        # so just a simple "assert expected == actual_grants" won't work
        diff_a = adapter.diff_of_grants(actual_grants, expected_grants_std)
        diff_b = adapter.diff_of_grants(expected_grants_std, actual_grants)
        assert diff_a == diff_b == {}


class BaseGrantsSnowflake(BaseGrantsSnowflakePatch, OrigBaseGrants):
    @pytest.fixture(scope="session", autouse=True)
    def ensure_database_roles(project):
        """
        We need to create database roles since test framework does not
        have default database roles to work with. This has been patched
        in with ta session scoped fixture and custom connection.
        """
        con = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_TEST_USER"),
            password=os.getenv("SNOWFLAKE_TEST_PASSWORD"),
            account=os.getenv("SNOWFLAKE_TEST_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_TEST_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_TEST_DATABASE"),
        )

        number_of_roles = 3

        for index in range(1, number_of_roles + 1):
            con.execute_string(f"CREATE DATABASE ROLE IF NOT EXISTS test_database_role_{index}")

        yield

        for index in range(1, number_of_roles + 1):
            con.execute_string(f"DROP DATABASE ROLE test_database_role_{index}")


class BaseCopyGrantsSnowflake:
    # Try every test case without copy_grants enabled (default),
    # and with copy_grants enabled (this base class)
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+copy_grants": True,
            },
            "seeds": {
                "+copy_grants": True,
            },
            "snapshots": {
                "+copy_grants": True,
            },
        }
