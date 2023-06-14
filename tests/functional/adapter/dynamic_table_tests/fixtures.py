from time import sleep
from datetime import datetime

import pytest
from snowflake.connector.errors import ProgrammingError

from dbt.dataclass_schema import StrEnum
from dbt.tests.util import relation_from_name, get_manifest
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


def refresh_dynamic_table(adapter, model: str):
    # there's no forced refresh, so just wait
    sleep(120)


def get_row_count(project, model: str) -> int:
    sql = f"select count(*) from {project.database}.{project.test_schema}.{model};"

    now = datetime.now()
    while (datetime.now() - now).total_seconds() < 5 * TARGET_LAG_IN_S:
        try:
            return project.run_sql(sql, fetch="one")[0]
        except ProgrammingError as err:
            not_initialized_msg = (
                "Please run a manual refresh or wait for a scheduled refresh before querying."
            )
            if not_initialized_msg in err.msg:
                sleep(5)
            else:
                raise err
    raise ProgrammingError(
        f"{5 * TARGET_LAG_IN_S} seconds has passed and the dynamic table is still not initialized."
    )


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
