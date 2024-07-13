from dbt.tests.util import run_dbt
import pytest


MODEL__LOGGING = """
import logging

import snowflake.snowpark as snowpark
import snowflake.snowpark.functions as f
from snowflake.snowpark.functions import *


logger = logging.getLogger("dbt_logger")
logger.info("******Inside Logging module.******")


def model(dbt, session: snowpark.Session):
    logger.info("******Logging start.******")
    df=session.sql(f"select current_user() as session_user, current_role() as session_role")
    logger.info("******Logging End.******")
    return df
"""


class TestPythonModelLogging:
    """
    This test case addresses bug report https://github.com/dbt-labs/dbt-snowflake/issues/846

    -- before running:
    USE ROLE ACCOUNTADMIN;
    ALTER ACCOUNT UNSET LOG_LEVEL;
    ALTER ACCOUNT UNSET TRACE_LEVEL;
    GRANT MODIFY SESSION LOG LEVEL ON ACCOUNT TO ROLE <DBT_ROLE>;
    GRANT MODIFY SESSION TRACE LEVEL ON ACCOUNT TO ROLE <DBT_ROLE>;
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {"logging_model.py": MODEL__LOGGING}

    def test_logging(self, project):
        run_dbt(["run"])
