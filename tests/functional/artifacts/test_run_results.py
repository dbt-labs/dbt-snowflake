import pytest
from dbt.tests.util import run_dbt

good_model_sql = """
select 1 as id
"""


class TestRunResultsSuccess:
    @pytest.fixture(scope="class")
    def models(self):
        return {"model.sql": good_model_sql}

    def test_timing_exists(self, project):
        results = run_dbt(["run"])
        assert len(results.results) == 1
        assert results.results[0].adapter_response["query_id"].strip() != ""
        assert results.results[0].adapter_response["session_id"].strip() != ""
