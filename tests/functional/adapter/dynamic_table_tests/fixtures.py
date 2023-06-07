from time import sleep

import pytest

from dbt.tests.util import relation_from_name, run_sql_with_adapter
from dbt.tests.adapter.materialized_view.base import Base
from dbt.tests.adapter.materialized_view.on_configuration_change import OnConfigurationChangeBase


class SnowflakeBasicBase(Base):
    @pytest.fixture(scope="class")
    def models(self):
        base_table = """
        {{ config(materialized='table') }}
        select 1 as base_column
        """
        base_dynamic_table = """
        {{ config(
            materialized='dynamic_table',
            warehouse='DBT_TESTING',
            lag='60 seconds',
        ) }}
        select * from {{ ref('base_table') }}
        """
        return {"base_table.sql": base_table, "base_dynamic_table.sql": base_dynamic_table}

    def refresh_dynamic_table(self, adapter):
        sql = "alter dynamic table base_dynamic_table set lag = '60 seconds'"
        run_sql_with_adapter(adapter, sql)
        sleep(90)


class SnowflakeOnConfigurationChangeBase(OnConfigurationChangeBase):
    # this avoids rewriting several log message lookups
    base_materialized_view = "base_dynamic_table"

    def refresh_dynamic_table(self, adapter):
        sql = "alter dynamic table base_dynamic_table set lag = '60 seconds'"
        run_sql_with_adapter(adapter, sql)

    @pytest.fixture(scope="class")
    def models(self):
        base_table = """
        {{ config(materialized='table') }}
        select 1 as base_column
        """
        base_dynamic_table = """
        {{ config(
            materialized='dynamic_table'
            warehouse='DBT_TESTING',
            lag='5 minutes',
        ) }}
        select * from {{ ref('base_table') }}
        """
        return {"base_table.sql": base_table, "base_dynamic_table.sql": base_dynamic_table}

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
