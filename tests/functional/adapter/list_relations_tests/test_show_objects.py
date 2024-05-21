import os
from typing import List

import pytest

from dbt.adapters.factory import get_adapter_by_type
from dbt.adapters.snowflake import SnowflakeRelation

from dbt.tests.util import run_dbt, get_connection


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


class TestShowObjects:
    views: int = 10
    tables: int = 10
    dynamic_tables: int = 10

    @pytest.fixture(scope="class")
    def seeds(self):
        yield {"my_seed.csv": SEED}

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

    @staticmethod
    def list_relations_without_caching(project) -> List[SnowflakeRelation]:
        my_adapter = get_adapter_by_type("snowflake")
        schema = my_adapter.Relation.create(
            database=project.database, schema=project.test_schema, identifier=""
        )
        with get_connection(my_adapter):
            relations = my_adapter.list_relations_without_caching(schema.without_identifier())
        return relations

    def test_list_relations_without_caching(self, project):
        relations = self.list_relations_without_caching(project)
        assert len([relation for relation in relations if relation.is_view]) == self.views
        assert (
            len([relation for relation in relations if relation.is_table])
            == self.tables + 1  # add the seed
        )
        assert (
            len([relation for relation in relations if relation.is_dynamic_table])
            == self.dynamic_tables
        )
