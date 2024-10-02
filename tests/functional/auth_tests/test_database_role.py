import os

import pytest

from dbt.tests.util import run_dbt


SEED = """
id
1
""".strip()


MODEL = """
{{ config(
    materialized='incremental',
) }}
select * from {{ ref('my_seed') }}
"""


class TestDatabaseRole:
    """
    This test addresses https://github.com/dbt-labs/dbt-snowflake/issues/1151

    While dbt-snowflake does not manage database roles (it only manages account roles,
    it still needs to account for them so that it doesn't try to revoke them.
    """

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"my_seed.csv": SEED}

    @pytest.fixture(scope="class")
    def models(self):
        return {"my_model.sql": MODEL}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        # grant to the test role even though this role already has these permissions
        # this triggers syncing grants since `apply_grants` first looks for a grants config
        return {"models": {"+grants": {"select": [os.getenv("SNOWFLAKE_TEST_ROLE")]}}}

    @pytest.fixture(scope="class", autouse=True)
    def setup(self, project, prefix):
        """
        Create a database role with access to the model we're about to create.
        The existence of this database role triggered the bug as dbt-snowflake attempts
        to revoke it if the user also provides a grants config.
        """
        role = f"BLOCKING_DB_ROLE_{prefix}"
        project.run_sql(f"CREATE DATABASE ROLE {role}")
        sql = f"""
        GRANT
            ALL PRIVILEGES ON FUTURE TABLES
            IN SCHEMA {project.test_schema}
            TO DATABASE ROLE {role}
        """
        project.run_sql(sql)
        yield
        project.run_sql(f"DROP DATABASE ROLE {role}")

    def test_database_role(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])
        # run a second time to trigger revoke on an incremental update
        # this originally failed, demonstrating the bug
        run_dbt(["run"])
