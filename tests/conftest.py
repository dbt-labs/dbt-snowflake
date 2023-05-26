import pytest
import os

# Import the fuctional fixtures as a plugin
# Note: fixtures with session scope need to be local

pytest_plugins = ["dbt.tests.fixtures.project"]


# The profile dictionary, used to write out profiles.yml
@pytest.fixture(scope="class")
def dbt_profile_target():
    return {
        "type": "snowflake",
        "threads": 4,
        "account": os.getenv("SNOWFLAKE_TEST_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_TEST_USER"),
        "password": os.getenv("SNOWFLAKE_TEST_PASSWORD"),
        "database": os.getenv("SNOWFLAKE_TEST_DATABASE"),
        "warehouse": os.getenv("SNOWFLAKE_TEST_WAREHOUSE"),
    }
