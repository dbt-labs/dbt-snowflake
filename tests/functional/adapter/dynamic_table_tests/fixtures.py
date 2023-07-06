import pytest

from dbt.tests.util import relation_from_name
from dbt.tests.adapter.materialized_view.base import Base
from dbt.tests.adapter.materialized_view.on_configuration_change import OnConfigurationChangeBase


BASE_TABLE = """
{{ config(materialized='table') }}
select 1 as base_column
"""


TARGET_LAG_IN_S = 60
BASE_DYNAMIC_TABLE = """
{{ config(
    materialized='dynamic_table',
    warehouse='DBT_TESTING',
    target_lag='60 seconds',
) }}
select * from {{ ref('base_table') }}
"""
BASE_DYNAMIC_TABLE_UPDATED = """
{{ config(
    materialized='dynamic_table'
    warehouse='DBT_TESTING',
    target_lag='120 seconds',
) }}
select * from {{ ref('base_table') }}
"""


class SnowflakeBasicBase(Base):
    @pytest.fixture(scope="class")
    def models(self):
        return {"base_table.sql": BASE_TABLE, "base_dynamic_table.sql": BASE_DYNAMIC_TABLE}


class SnowflakeOnConfigurationChangeBase(OnConfigurationChangeBase):
    # this avoids rewriting several log message lookups
    base_materialized_view = "base_dynamic_table"

    @pytest.fixture(scope="class")
    def models(self):
        return {"base_table.sql": BASE_TABLE, "base_dynamic_table.sql": BASE_DYNAMIC_TABLE_UPDATED}

    @pytest.fixture(scope="function")
    def configuration_changes(self, project):
        pass

    @pytest.fixture(scope="function")
    def configuration_change_message(self, project):
        # We need to do this because the default quote policy is overriden
        # in `SnowflakeAdapter.list_relations_without_caching`; we wind up with
        # an uppercase quoted name when supplied with a lowercase name with un-quoted quote policy.
        relation = relation_from_name(project.adapter, "base_dynamic_table")
        database, schema, name = str(relation).split(".")
        relation_upper = f'"{database.upper()}"."{schema.upper()}"."{name.upper()}"'
        return f"Determining configuration changes on: {relation_upper}"
