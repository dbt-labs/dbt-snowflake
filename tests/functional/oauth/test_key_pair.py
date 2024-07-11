import os

from dbt.tests.util import run_dbt
import pytest


class TestKeyPairAuth:
    @pytest.fixture(scope="class", autouse=True)
    def dbt_profile_target(self):
        return {
            "type": "snowflake",
            "threads": 4,
            "account": os.getenv("SNOWFLAKE_TEST_ACCOUNT"),
            "user": os.getenv("SNOWFLAKE_TEST_USER"),
            "private_key": os.getenv("SNOWFLAKE_TEST_PRIVATE_KEY"),
            "private_key_passphrase": os.getenv("SNOWFLAKE_TEST_PRIVATE_KEY_PASSPHRASE"),
            "database": os.getenv("SNOWFLAKE_TEST_DATABASE"),
            "warehouse": os.getenv("SNOWFLAKE_TEST_WAREHOUSE"),
            "authenticator": "oauth",
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"model.sql": "select 1 as id"}

    def test_snowflake_basic(self, project):
        run_dbt()
