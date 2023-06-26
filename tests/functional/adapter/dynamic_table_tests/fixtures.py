import pytest
from dbt.tests.util import run_dbt, relation_from_name as _relation_from_name
from dbt.tests.adapter.materialized_view.base import Base
from dbt.tests.adapter.materialized_view.on_configuration_change import (
    get_model_file,
    set_model_file,
)


BASE_TABLE = """
{{ config(materialized='table') }}
select 1 as base_column
"""
BASE_DYNAMIC_TABLE = """
{{ config(
    materialized='dynamic_table',
    warehouse='DBT_TESTING',
    target_lag='1 minute',
) }}
select * from {{ ref('base_table') }}
"""


def relation_from_name(adapter, name: str) -> str:
    relation = _relation_from_name(adapter, name)
    return str(relation).upper()


class SnowflakeBasicBase(Base):
    @pytest.fixture(scope="function", autouse=True)
    def setup(self, project, adapter):
        run_dbt(["run", "--full-refresh"])

    @pytest.fixture(scope="class")
    def models(self):
        return {"base_table.sql": BASE_TABLE, "base_dynamic_table.sql": BASE_DYNAMIC_TABLE}


class SnowflakeOnConfigurationChangeBase(Base):
    base_dynamic_table = "base_dynamic_table"

    @pytest.fixture(scope="function", autouse=True)
    def setup(self, project, adapter):
        run_dbt(["run", "--full-refresh"])

    @pytest.fixture(scope="class")
    def models(self):
        return {"base_table.sql": BASE_TABLE, f"{self.base_dynamic_table}.sql": BASE_DYNAMIC_TABLE}

    @pytest.fixture(scope="function")
    def configuration_changes_apply(self, project):
        initial_model = get_model_file(project, self.base_dynamic_table)

        # change the index from [`id`, `value`] to [`new_id`, `new_value`]
        new_model = initial_model.replace(
            "target_lag='1 minute'",
            "target_lag='2 minutes'",
        )
        set_model_file(project, self.base_dynamic_table, new_model)

        yield

        # set this back and persist to the database for the next test
        set_model_file(project, self.base_dynamic_table, initial_model)

    @pytest.fixture(scope="function")
    def alter_message(self, project):
        return f"Applying ALTER to: {relation_from_name(project.adapter, self.base_dynamic_table)}"

    @pytest.fixture(scope="function")
    def create_message(self, project):
        return (
            f"Applying CREATE to: {relation_from_name(project.adapter, self.base_dynamic_table)}"
        )

    @pytest.fixture(scope="function")
    def refresh_message(self, project):
        return (
            f"Applying REFRESH to: {relation_from_name(project.adapter, self.base_dynamic_table)}"
        )

    @pytest.fixture(scope="function")
    def replace_message(self, project):
        return (
            f"Applying REPLACE to: {relation_from_name(project.adapter, self.base_dynamic_table)}"
        )

    @pytest.fixture(scope="function")
    def configuration_change_continue_message(self, project):
        return (
            f"Configuration changes were identified and `on_configuration_change` "
            f"was set to `continue` for `{relation_from_name(project.adapter, self.base_dynamic_table)}`"
        )

    @pytest.fixture(scope="function")
    def configuration_change_fail_message(self, project):
        return (
            f"Configuration changes were identified and `on_configuration_change` "
            f"was set to `fail` for `{relation_from_name(project.adapter, self.base_dynamic_table)}`"
        )

    @pytest.fixture(scope="function")
    def update_target_lag_message(self, project):
        return f"Applying UPDATE TARGET_LAG to: {relation_from_name(project.adapter, self.base_dynamic_table)}"
