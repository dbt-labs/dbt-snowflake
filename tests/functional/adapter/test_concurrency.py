import pytest
from pathlib import Path
from dbt.tests.util import (
    run_dbt,
    check_relations_equal,
    run_dbt_and_capture,
    run_sql_with_adapter,
    )
from dbt.tests.adapter.concurrency.test_concurrency import BaseConcurrency

class TestConncurenncySnowflake(BaseConcurrency):
    def test_conncurrency_snowflake(self, project):
        results = run_dbt(['run'], expect_pass=False)
        assert len(results) == 7
        check_relations_equal(project.adapter, ["SEED", "VIEW_MODEL"])
        check_relations_equal(project.adapter, ["SEED", "DEP"])
        check_relations_equal(project.adapter, ["SEED", "TABLE_A"])
        check_relations_equal(project.adapter, ["SEED", "TABLE_B"])
        run_sql_with_adapter(project.test_data_dir / Path("update.sql"))

        results = run_dbt_and_capture(["run"], expect_pass=False)
        assert len(results) == 7

        check_relations_equal(project.adapter, ["SEED", "VIEW_MODEL"])
        check_relations_equal(project.adapter, ["SEED", "DEP"])
        check_relations_equal(project.adapter, ["SEED", "TABLE_A"])
        check_relations_equal(project.adapter, ["SEED", "TABLE_B"])
