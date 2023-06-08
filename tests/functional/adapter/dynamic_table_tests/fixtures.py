from time import sleep
from datetime import datetime

import pytest
from snowflake.connector.errors import ProgrammingError

from dbt.dataclass_schema import StrEnum
from dbt.tests.util import relation_from_name, run_sql_with_adapter, get_manifest
from dbt.tests.adapter.materialized_view.base import Base
from dbt.tests.adapter.materialized_view.on_configuration_change import OnConfigurationChangeBase


def refresh_dynamic_table(adapter, model: str):
    sql = f"alter dynamic table {model} set lag = '60 seconds'"
    run_sql_with_adapter(adapter, sql)


def get_row_count(project, model: str) -> int:
    sql = f"select count(*) from {project.database}.{project.test_schema}.{model};"

    now = datetime.now()
    while (datetime.now() - now).total_seconds() < 120:
        try:
            return project.run_sql(sql, fetch="one")[0]
        except ProgrammingError:
            sleep(5)
    raise ProgrammingError("90 seconds has passed and the dynamic table is still not initialized.")


def assert_model_exists_and_is_correct_type(project, model: str, relation_type: StrEnum):
    # In general, `relation_type` will be of type `RelationType`.
    # However, in some cases (e.g. `dbt-snowflake`) adapters will have their own `RelationType`.
    manifest = get_manifest(project.project_root)
    model_metadata = manifest.nodes[f"model.test.{model}"]
    assert model_metadata.config.materialized == relation_type
    assert get_row_count(project, model) >= 0


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
