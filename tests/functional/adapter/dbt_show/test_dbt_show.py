import pytest
from dbt.tests.util import run_dbt
from dbt.tests.adapter.dbt_show.test_dbt_show import BaseShowSqlHeader, BaseShowLimit

seeds__sample_seed = """sample_num,sample_bool
1,true
4,false
2,false
5,true
6,false
7,true
3,true
"""

models__sample_model_with_order_by = """
select * from {{ ref('sample_seed') }} order by sample_num
"""


class TestSnowflakeShowLimit(BaseShowLimit):
    pass


class TestSnowflakeShowSqlHeader(BaseShowSqlHeader):
    pass


class TestSnowflakeShowLimitOrderBy:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "sample_model.sql": models__sample_model_with_order_by,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"sample_seed.csv": seeds__sample_seed}

    def test_limit(self, project):
        run_dbt(["build"])
        dbt_args = ["show", "--inline", "sample_model", "--limit", "5"]
        results = run_dbt(dbt_args)
        result_table = results.results[0].agate_table
        assert result_table[0][0] == 1
        assert result_table[4][0] == 5
