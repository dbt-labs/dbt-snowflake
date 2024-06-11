"""
Please follow the instructions in test_oauth.py for instructions on how to set up
the security integration required to retrieve a JWT from Snowflake.
"""

import pytest
import os
from dbt.tests.util import run_dbt, check_relations_equal

from dbt.adapters.snowflake import SnowflakeCredentials

_MODELS__MODEL_1_SQL = """
select 1 as id
"""


_MODELS__MODEL_2_SQL = """
select 2 as id
"""


_MODELS__MODEL_3_SQL = """
select * from {{ ref('model_1') }}
union all
select * from {{ ref('model_2') }}
"""


_MODELS__MODEL_4_SQL = """
select 1 as id
union all
select 2 as id
"""


class TestSnowflakeJWT:
    """Tests that setting authenticator: jwt allows setting token to a plain JWT
    that will be passed into the Snowflake connection without modification."""

    @pytest.fixture(scope="class", autouse=True)
    def access_token(self):
        """Because JWTs are short-lived, we need to get a fresh JWT via the refresh
        token flow before running the test.

        This fixture leverages the existing SnowflakeCredentials._get_access_token
        method to retrieve a valid JWT from Snowflake.
        """
        client_id = os.getenv("SNOWFLAKE_TEST_OAUTH_CLIENT_ID")
        client_secret = os.getenv("SNOWFLAKE_TEST_OAUTH_CLIENT_SECRET")
        refresh_token = os.getenv("SNOWFLAKE_TEST_OAUTH_REFRESH_TOKEN")

        credentials = SnowflakeCredentials(
            account=os.getenv("SNOWFLAKE_TEST_ACCOUNT"),
            database="",
            schema="",
            authenticator="oauth",
            oauth_client_id=client_id,
            oauth_client_secret=client_secret,
            token=refresh_token,
        )

        yield credentials._get_access_token()

    @pytest.fixture(scope="class", autouse=True)
    def dbt_profile_target(self, access_token):
        """A dbt_profile that has authenticator set to JWT, and token set to
        a JWT accepted by Snowflake. Also omits the user, as the user attribute
        is optional when the authenticator is set to JWT.
        """
        return {
            "type": "snowflake",
            "threads": 4,
            "account": os.getenv("SNOWFLAKE_TEST_ACCOUNT"),
            "database": os.getenv("SNOWFLAKE_TEST_DATABASE"),
            "warehouse": os.getenv("SNOWFLAKE_TEST_WAREHOUSE"),
            "authenticator": "jwt",
            "token": access_token,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_1.sql": _MODELS__MODEL_1_SQL,
            "model_2.sql": _MODELS__MODEL_2_SQL,
            "model_3.sql": _MODELS__MODEL_3_SQL,
            "model_4.sql": _MODELS__MODEL_4_SQL,
        }

    def test_snowflake_basic(self, project):
        run_dbt()
        check_relations_equal(project.adapter, ["MODEL_3", "MODEL_4"])
