from typing import Optional, Tuple

import pytest

from dbt.tests.util import (
    assert_message_in_logs,
    get_model_file,
    run_dbt,
    run_dbt_and_capture,
    set_model_file,
)

from dbt.adapters.snowflake.relation import SnowflakeRelation, SnowflakeRelationType
from tests.functional.adapter.dynamic_table_tests.files import (
    MY_DYNAMIC_TABLE,
    MY_SEED,
    MY_TABLE,
    MY_VIEW,
)
from tests.functional.adapter.dynamic_table_tests.utils import query_relation_type


class TestSnowflakeDynamicTableBasic:
    @staticmethod
    def insert_record(project, table: SnowflakeRelation, record: Tuple[int, int]):
        my_id, value = record
        project.run_sql(f"insert into {table} (id, value) values ({my_id}, {value})")

    @staticmethod
    def refresh_dynamic_table(project, dynamic_table: SnowflakeRelation):
        sql = f"alter dynamic table {dynamic_table} refresh"
        project.run_sql(sql)

    @staticmethod
    def query_row_count(project, relation: SnowflakeRelation) -> int:
        sql = f"select count(*) from {relation}"
        return project.run_sql(sql, fetch="one")[0]

    @staticmethod
    def query_relation_type(project, relation: SnowflakeRelation) -> Optional[str]:
        return query_relation_type(project, relation)

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"my_seed.csv": MY_SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "my_table.sql": MY_TABLE,
            "my_view.sql": MY_VIEW,
            "my_dynamic_table.sql": MY_DYNAMIC_TABLE,
        }

    @pytest.fixture(scope="class")
    def my_dynamic_table(self, project) -> SnowflakeRelation:
        return project.adapter.Relation.create(
            identifier="my_dynamic_table",
            schema=project.test_schema,
            database=project.database,
            type=SnowflakeRelationType.DynamicTable,
        )

    @pytest.fixture(scope="class")
    def my_view(self, project) -> SnowflakeRelation:
        return project.adapter.Relation.create(
            identifier="my_view",
            schema=project.test_schema,
            database=project.database,
            type=SnowflakeRelationType.View,
        )

    @pytest.fixture(scope="class")
    def my_table(self, project) -> SnowflakeRelation:
        return project.adapter.Relation.create(
            identifier="my_table",
            schema=project.test_schema,
            database=project.database,
            type=SnowflakeRelationType.Table,
        )

    @pytest.fixture(scope="class")
    def my_seed(self, project) -> SnowflakeRelation:
        return project.adapter.Relation.create(
            identifier="my_seed",
            schema=project.test_schema,
            database=project.database,
            type=SnowflakeRelationType.Table,
        )

    @staticmethod
    def swap_table_to_dynamic_table(project, table):
        initial_model = get_model_file(project, table)
        new_model = initial_model.replace("materialized='table'", "materialized='dynamic_table'")
        set_model_file(project, table, new_model)

    @staticmethod
    def swap_view_to_dynamic_table(project, view):
        initial_model = get_model_file(project, view)
        new_model = initial_model.replace("materialized='view'", "materialized='dynamic_table'")
        set_model_file(project, view, new_model)

    @staticmethod
    def swap_dynamic_table_to_table(project, dynamic_table):
        initial_model = get_model_file(project, dynamic_table)
        new_model = initial_model.replace("materialized='dynamic_table'", "materialized='table'")
        set_model_file(project, dynamic_table, new_model)

    @staticmethod
    def swap_dynamic_table_to_view(project, dynamic_table):
        initial_model = get_model_file(project, dynamic_table)
        new_model = initial_model.replace("materialized='dynamic_table'", "materialized='view'")
        set_model_file(project, dynamic_table, new_model)

    @pytest.fixture(scope="function", autouse=True)
    def setup(self, project, my_dynamic_table):
        run_dbt(["seed"])
        run_dbt(["run", "--models", my_dynamic_table.identifier, "--full-refresh"])

        # the tests touch these files, store their contents in memory
        initial_model = get_model_file(project, my_dynamic_table)

        yield

        # and then reset them after the test runs
        set_model_file(project, my_dynamic_table, initial_model)
        project.run_sql(f"drop schema if exists {project.test_schema} cascade")

    def test_dynamic_table_create(self, project, my_dynamic_table):
        # setup creates it; verify it's there
        assert self.query_relation_type(project, my_dynamic_table) == "dynamic_table"

    def test_dynamic_table_create_idempotent(self, project, my_dynamic_table):
        # setup creates it once; verify it's there and run once
        assert self.query_relation_type(project, my_dynamic_table) == "dynamic_table"
        run_dbt(["run", "--models", my_dynamic_table.identifier])
        assert self.query_relation_type(project, my_dynamic_table) == "dynamic_table"

    def test_dynamic_table_full_refresh(self, project, my_dynamic_table):
        _, logs = run_dbt_and_capture(
            ["--debug", "run", "--models", my_dynamic_table.identifier, "--full-refresh"]
        )
        assert self.query_relation_type(project, my_dynamic_table) == "dynamic_table"
        assert_message_in_logs(f"Applying REPLACE to: {my_dynamic_table}", logs)

    @pytest.mark.skip(
        "The current implementation does not support overwriting tables with dynamic tables."
    )
    def test_dynamic_table_replaces_table(self, project, my_table):
        run_dbt(["run", "--models", my_table.identifier])
        assert self.query_relation_type(project, my_table) == "table"

        self.swap_table_to_dynamic_table(project, my_table)

        run_dbt(["run", "--models", my_table.identifier])
        assert self.query_relation_type(project, my_table) == "dynamic_table"

    @pytest.mark.skip(
        "The current implementation does not support overwriting views with dynamic tables."
    )
    def test_dynamic_table_replaces_view(self, project, my_view):
        run_dbt(["run", "--models", my_view.identifier])
        assert self.query_relation_type(project, my_view) == "view"

        self.swap_view_to_dynamic_table(project, my_view)

        run_dbt(["run", "--models", my_view.identifier])
        assert self.query_relation_type(project, my_view) == "dynamic_table"

    @pytest.mark.skip(
        "The current implementation does not support overwriting dynamic tables with tables."
    )
    def test_table_replaces_dynamic_table(self, project, my_dynamic_table):
        run_dbt(["run", "--models", my_dynamic_table.identifier])
        assert self.query_relation_type(project, my_dynamic_table) == "dynamic_table"

        self.swap_dynamic_table_to_table(project, my_dynamic_table)

        run_dbt(["run", "--models", my_dynamic_table.identifier])
        assert self.query_relation_type(project, my_dynamic_table) == "table"

    @pytest.mark.skip(
        "The current implementation does not support overwriting dynamic tables with views."
    )
    def test_view_replaces_dynamic_table(self, project, my_dynamic_table):
        run_dbt(["run", "--models", my_dynamic_table.identifier])
        assert self.query_relation_type(project, my_dynamic_table) == "dynamic_table"

        self.swap_dynamic_table_to_view(project, my_dynamic_table)

        run_dbt(["run", "--models", my_dynamic_table.identifier])
        assert self.query_relation_type(project, my_dynamic_table) == "view"

    def test_dynamic_table_only_updates_after_refresh(self, project, my_dynamic_table, my_seed):
        # poll database
        table_start = self.query_row_count(project, my_seed)
        view_start = self.query_row_count(project, my_dynamic_table)

        # insert new record in table
        self.insert_record(project, my_seed, (4, 400))

        # poll database
        table_mid = self.query_row_count(project, my_seed)
        view_mid = self.query_row_count(project, my_dynamic_table)

        # refresh the materialized view
        self.refresh_dynamic_table(project, my_dynamic_table)

        # poll database
        table_end = self.query_row_count(project, my_seed)
        view_end = self.query_row_count(project, my_dynamic_table)

        # new records were inserted in the table but didn't show up in the view until it was refreshed
        assert table_start < table_mid == table_end
        assert view_start == view_mid < view_end
