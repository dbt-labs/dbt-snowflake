from datetime import datetime, timedelta
import os
from statistics import mean
from typing import List, Tuple

import pytest

from dbt.adapters.factory import get_adapter_by_type
from dbt.adapters.snowflake import SnowflakeRelation

from dbt.tests.util import run_dbt, get_connection


MY_SEED = """
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
    target_lag='1 minute',
    snowflake_warehouse='"""
    + os.getenv("SNOWFLAKE_TEST_WAREHOUSE")
    + """',
) }}
select * from {{ ref('my_seed') }}
"""
)


SHOW_OBJECTS = """
{% macro snowflake__get_show_objects_sql(schema, results_per_iteration) %}
    show objects in {{ schema.database }}.{{ schema.schema }} limit {{ results_per_iteration }}
{% endmacro %}
"""


SHOW_TERSE_OBJECTS = """
{% macro snowflake__get_show_objects_sql(schema, results_per_iteration) %}
    show terse objects in {{ schema.database }}.{{ schema.schema }} limit {{ results_per_iteration }}
{% endmacro %}
"""


class ListRelations:
    views: int = 100
    tables: int = 100
    dynamic_tables: int = 100
    iterations: int = 10
    expected_duration: float

    @pytest.fixture(scope="class")
    def seeds(self):
        yield {"my_seed.csv": MY_SEED}

    @pytest.fixture(scope="class")
    def models(self):
        models = {}
        models.update({f"my_view_{i}.sql": VIEW for i in range(self.views)})
        models.update({f"my_table_{i}.sql": TABLE for i in range(self.tables)})
        models.update(
            {f"my_dynamic_table_{i}.sql": DYNAMIC_TABLE for i in range(self.dynamic_tables)}
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
        with get_connection(my_adapter) as conn:
            relations = my_adapter.list_relations_without_caching(schema)
        end = datetime.utcnow()
        duration = end - start
        return relations, duration

    def test_show_terse_objects(self, project):
        durations = []
        for i in range(self.iterations):
            relations, duration = self.list_relations(project)
            durations.append(duration.total_seconds())
            assert len([relation for relation in relations if relation.is_view]) == self.views
            assert (
                len([relation for relation in relations if relation.is_table]) == self.tables + 1
            )  # add the seed
            assert (
                len([relation for relation in relations if relation.is_dynamic_table])
                == self.dynamic_tables
            )
        assert mean(durations) < self.expected_duration


class TestShowObjects(ListRelations):
    expected_duration = timedelta(
        seconds=1, microseconds=100_000
    ).total_seconds()  # allows 10% error

    @pytest.fixture(scope="class")
    def macros(self):
        yield {"snowflake__get_show_objects_sql.sql": SHOW_OBJECTS}


class TestShowTerseObjects(ListRelations):
    expected_duration = timedelta(seconds=1, microseconds=0).total_seconds()  # allows 10% error

    @pytest.fixture(scope="class")
    def macros(self):
        yield {"snowflake__get_show_objects_sql.sql": SHOW_TERSE_OBJECTS}
