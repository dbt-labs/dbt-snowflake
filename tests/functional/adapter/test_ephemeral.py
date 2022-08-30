import pytest
from dbt.tests.adapter.ephemeral.test_ephemeral import BaseEphemeralMulti
from dbt.tests.util import run_dbt, check_relations_equal

class TestEphemeralMultiSnowflake(BaseEphemeralMulti):

    def test_ephemeral_multi_snowflake(self, project):
        results = run_dbt(["run"])
        assert len(results) == 3
        check_relations_equal(project.adapter, ["seed", "dependent", "double_dependent", "super_dependent"])