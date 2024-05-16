from dataclasses import dataclass
from datetime import datetime, timedelta
import os
from statistics import mean
from typing import List, Tuple

import pytest

from dbt.adapters.factory import get_adapter_by_type
from dbt.adapters.snowflake import SnowflakeRelation

from dbt.tests.util import run_dbt, get_connection
from tests.performance.conftest import performance_test


SEED = """
id,value
0,red
1,yellow
2,blue
""".strip()


VIEW = """
select * from {{ ref('my_seed') }}
"""


TABLE = """
{{ config(materialized='table') }}
select * from {{ ref('my_seed') }}
"""


DYNAMIC_TABLE = (
    """
{{ config(
    materialized='dynamic_table',
    target_lag='1 day',
    snowflake_warehouse='"""
    + os.getenv("SNOWFLAKE_TEST_WAREHOUSE")
    + """',
) }}
select * from {{ ref('my_seed') }}
"""
)


@dataclass
class Scenario:
    views: int
    tables: int
    dynamic_tables: int


class BaseConfig:
    scenario: Scenario
    expected_duration: float
    iterations: int = 10

    @pytest.fixture(scope="class")
    def seeds(self):
        yield {"my_seed.csv": SEED}

    @pytest.fixture(scope="class")
    def models(self):
        models = {}
        models.update({f"my_view_{i}.sql": VIEW for i in range(self.scenario.views)})
        models.update({f"my_table_{i}.sql": TABLE for i in range(self.scenario.tables)})
        models.update(
            {
                f"my_dynamic_table_{i}.sql": DYNAMIC_TABLE
                for i in range(self.scenario.dynamic_tables)
            }
        )
        yield models

    @pytest.fixture(scope="class", autouse=True)
    def setup(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])

    def list_relations(self, project) -> Tuple[List[SnowflakeRelation], timedelta]:
        my_adapter = get_adapter_by_type("snowflake")
        schema = my_adapter.Relation.create(
            database=project.database, schema=project.test_schema, identifier=""
        )

        start = datetime.utcnow()
        with get_connection(my_adapter):
            relations = my_adapter.list_relations_without_caching(schema)
        end = datetime.utcnow()
        duration = end - start
        return relations, duration

    @performance_test
    def test_list_relations(self, project):
        durations = []
        for i in range(self.iterations):
            relations, duration = self.list_relations(project)
            durations.append(duration.total_seconds())
            assert (
                len([relation for relation in relations if relation.is_view])
                == self.scenario.views
            )
            assert (
                len([relation for relation in relations if relation.is_table])
                == self.scenario.tables + 1  # add the seed
            )
            assert (
                len([relation for relation in relations if relation.is_dynamic_table])
                == self.scenario.dynamic_tables
            )
        assert mean(durations) < self.expected_duration * 1.10  # allow for 10% error
