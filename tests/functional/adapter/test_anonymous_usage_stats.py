from dbt.tests.util import run_dbt_and_capture
import pytest


ANONYMOUS_USAGE_MESSAGE = """
sys._xoptions['snowflake_partner_attribution'].append("dbtLabs_dbtPython")
""".strip()


MY_PYTHON_MODEL = """
import pandas

def model(dbt, session):
    dbt.config(materialized='table')
    data = [[1,2]] * 10
    return pandas.DataFrame(data, columns=['test', 'test2'])
"""


class AnonymousUsageStatsBase:
    @pytest.fixture(scope="class")
    def models(self):
        return {"my_python_model.py": MY_PYTHON_MODEL}


class TestAnonymousUsageStatsOn(AnonymousUsageStatsBase):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"send_anonymous_usage_stats": True}}

    def test_stats_get_sent(self, project):
        _, logs = run_dbt_and_capture(["--debug", "run"])
        assert ANONYMOUS_USAGE_MESSAGE in logs


class TestAnonymousUsageStatsOff(AnonymousUsageStatsBase):
    @pytest.fixture(scope="class")
    def project_config_update(self, dbt_profile_target):
        return {"flags": {"send_anonymous_usage_stats": False}}

    def test_stats_do_not_get_sent(self, project):
        _, logs = run_dbt_and_capture(["--debug", "run"])
        assert ANONYMOUS_USAGE_MESSAGE not in logs
