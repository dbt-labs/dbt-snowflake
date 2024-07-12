"""
Results:

| method        | project_size | reuse_connections | unsafe_skip_rsa_key_validation | duration |
|---------------|--------------|-------------------|--------------------------------|----------|
| User Password |        1,000 | False             | -                              |  234.09s |
| User Password |        1,000 | True              | -                              |   78.34s |
| Key Pair      |        1,000 | False             | False                          |  271.47s |
| Key Pair      |        1,000 | False             | True                           |  275.73s |
| Key Pair      |        1,000 | True              | False                          |   63.69s |
| Key Pair      |        1,000 | True              | True                           |   73.45s |

Notes:
- run locally on MacOS, single threaded
- `unsafe_skip_rsa_key_validation` only applies to the Key Pair auth method
- `unsafe_skip_rsa_key_validation=True` was tested by updating the relevant `cryptography` calls directly as it is not a user configuration
- since the models are all views, time differences should be viewed as absolute differences, e.g.:
    - this: (271.47s - 63.69s) / 1,000 models = 208ms improvement
    - NOT this: 1 - (63.69s / 271.47s) = 76.7% improvement
"""

from datetime import datetime
import os

from dbt.tests.util import run_dbt
import pytest


SEED = """
id,value
1,a
2,b
3,c
""".strip()


MODEL = """
select * from {{ ref("my_seed") }}
"""


class Scenario:
    """
    Runs a full load test. The test can be configured to run an arbitrary number of models.

    To use this test, configure the test by setting `project_size` and/or `expected_duration`.
    """

    auth_method: str
    project_size: int = 1
    reuse_connections: bool = False

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"my_seed.csv": SEED}

    @pytest.fixture(scope="class")
    def models(self):
        return {f"my_model_{i}.sql": MODEL for i in range(self.project_size)}

    @pytest.fixture(scope="class", autouse=True)
    def setup(self, project):
        run_dbt(["seed"])

        start = datetime.now()
        yield
        end = datetime.now()

        duration = (end - start).total_seconds()
        print(f"Run took: {duration} seconds")

    @pytest.fixture(scope="class")
    def dbt_profile_target(self, auth_params):
        yield {
            "type": "snowflake",
            "threads": 4,
            "account": os.getenv("SNOWFLAKE_TEST_ACCOUNT"),
            "database": os.getenv("SNOWFLAKE_TEST_DATABASE"),
            "warehouse": os.getenv("SNOWFLAKE_TEST_WAREHOUSE"),
            "user": os.getenv("SNOWFLAKE_TEST_USER"),
            "reuse_connections": self.reuse_connections,
            **auth_params,
        }

    @pytest.fixture(scope="class")
    def auth_params(self):

        if self.auth_method == "user_password":
            yield {"password": os.getenv("SNOWFLAKE_TEST_PASSWORD")}

        elif self.auth_method == "key_pair":
            """
            This connection method uses key pair auth.
            Follow the instructions here to setup key pair authentication for your test user:
            https://docs.snowflake.com/en/user-guide/key-pair-auth
            """
            yield {
                "private_key": os.getenv("SNOWFLAKE_TEST_PRIVATE_KEY"),
                "private_key_passphrase": os.getenv("SNOWFLAKE_TEST_PRIVATE_KEY_PASSPHRASE"),
            }

        else:
            raise ValueError(
                f"`auth_method` must be one of `user_password` or `key_pair`, received: {self.auth_method}"
            )

    def test_scenario(self, project):
        run_dbt(["run"])


class TestUserPasswordAuth(Scenario):
    auth_method = "user_password"
    project_size = 1_000
    reuse_connections = False


class TestUserPasswordAuthReuseConnections(Scenario):
    auth_method = "user_password"
    project_size = 1_000
    reuse_connections = True


class TestKeyPairAuth(Scenario):
    auth_method = "key_pair"
    project_size = 1_000
    reuse_connections = False


class TestKeyPairAuthReuseConnections(Scenario):
    auth_method = "key_pair"
    project_size = 1_000
    reuse_connections = True
