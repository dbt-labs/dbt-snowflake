import pytest
from pathlib import Path
from dbt.tests.util import (
    run_dbt,
    check_relations_equal
    )
from dbt.tests.adapter.concurrency.test_concurrency import BaseConcurrency

class TestConncurenncySnowflake(BaseConcurrency):
    def test_conncurrency_snowflake(self, project):
        run_dbt(["seed", "--select", "seed"])
        results = run_dbt(["run"], expect_pass=False)
        assert len(results) == 7
        check_relations_equal(project.adapter, ["SEED", "VIEW_MODEL"])
        check_relations_equal(project.adapter, ["SEED", "DEP"])
        check_relations_equal(project.adapter, ["SEED", "TABLE_A"])
        check_relations_equal(project.adapter, ["SEED", "TABLE_B"])

        run_dbt(["seed", "--select", "update"])
        results = run_dbt(["run"], expect_pass=False)
        assert len(results) == 7
        check_relations_equal(project.adapter, ["SEED", "VIEW_MODEL"])
        check_relations_equal(project.adapter, ["SEED", "DEP"])
        check_relations_equal(project.adapter, ["SEED", "TABLE_A"])
        check_relations_equal(project.adapter, ["SEED", "TABLE_B"])
