from typing import Optional, Tuple
import pytest

from dbt.tests.util import (
    get_model_file,
    run_dbt,
    set_model_file,
)
from dbt.tests.fixtures.project import TestProjInfo

from dbt.adapters.snowflake.relation import SnowflakeRelation
from dbt.adapters.snowflake.relation_configs.policies import SnowflakeRelationType
from tests.functional.adapter.materialized_view_tests.files import (
    MY_MATERIALIZED_VIEW,
    MY_SEED,
    MY_TABLE,
)


class TestSnowflakeMaterializedViews:
    @staticmethod
    def insert_record(project: TestProjInfo, table: SnowflakeRelation, record: Tuple[int, int]):
        my_id, value = record
        project.run_sql(f"insert into {table} (id, value) values ({my_id}, {value})")

    @staticmethod
    def query_relation_type(project: TestProjInfo, relation: SnowflakeRelation) -> Optional[str]:
        sql = f"""
            select
                case
                    when table_type = 'MATERIALIZED VIEW' then 'materialized_view'
                end as relation_type
            from information_schema.tables
            where table_name like '{relation.identifier.upper()}'
            and table_schema like '{relation.schema.upper()}'
            and table_catalog like '{relation.database.upper()}'
        """
        results = project.run_sql(sql, fetch="one")
        if results is None or len(results) == 0:
            return None
        elif len(results) > 1:
            raise ValueError(f"More than one instance of {relation.name} found!")
        else:
            return results[0].lower()

    @staticmethod
    def query_row_count(project: TestProjInfo, relation: SnowflakeRelation) -> int:
        sql = f"select count(*) from {relation}"
        return project.run_sql(sql, fetch="one")[0]

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"my_seed.csv": MY_SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "my_table.sql": MY_TABLE,
            "my_materialized_view.sql": MY_MATERIALIZED_VIEW,
        }

    @pytest.fixture(scope="class")
    def my_table(self, project: TestProjInfo) -> SnowflakeRelation:
        return project.adapter.Relation.create(
            identifier="my_table",
            schema=project.test_schema,
            database=project.database,
            type=SnowflakeRelationType.Table,
        )

    @pytest.fixture(scope="class")
    def my_seed(self, project: TestProjInfo) -> SnowflakeRelation:
        return project.adapter.Relation.create(
            identifier="my_seed",
            schema=project.test_schema,
            database=project.database,
            type=SnowflakeRelationType.Table,
        )

    @pytest.fixture(scope="class")
    def my_materialized_view(self, project: TestProjInfo) -> SnowflakeRelation:
        return project.adapter.Relation.create(
            identifier="my_materialized_view",
            schema=project.test_schema,
            database=project.database,
            type=SnowflakeRelationType.MaterializedView,
        )

    @staticmethod
    def load_model(
        project: TestProjInfo,
        current_model: SnowflakeRelation,
        new_model: SnowflakeRelation,
    ):
        model_to_load = get_model_file(project, new_model)
        set_model_file(project, current_model, model_to_load)

    @pytest.fixture(scope="function", autouse=True)
    def setup(
        self,
        project: TestProjInfo,
        my_materialized_view: SnowflakeRelation,
        my_table: SnowflakeRelation,
    ):
        run_dbt(["seed"])
        run_dbt(["run", "--models", my_materialized_view.identifier, "--full-refresh"])

        # the tests touch these files, store their contents in memory
        my_materialized_view_config = get_model_file(project, my_materialized_view)
        my_table_config = get_model_file(project, my_table)

        yield

        # and then reset them after the test runs
        set_model_file(project, my_materialized_view, my_materialized_view_config)
        set_model_file(project, my_table, my_table_config)
        project.run_sql(f"drop schema if exists {project.test_schema} cascade")

    def test_materialized_view_create(
        self, project: TestProjInfo, my_materialized_view: SnowflakeRelation
    ):
        # setup creates it; verify it's there
        assert self.query_relation_type(project, my_materialized_view) == "materialized_view"

    def test_materialized_view_update_on_insert(
        self,
        project: TestProjInfo,
        my_table: SnowflakeRelation,
        my_materialized_view: SnowflakeRelation,
    ):
        table_count_start = self.query_row_count(project, my_table)
        view_count_start = self.query_row_count(project, my_materialized_view)

        assert table_count_start == view_count_start

        self.insert_record(project, my_table, (4, 400))

        table_count_end = self.query_row_count(project, my_table)
        view_count_end = self.query_row_count(project, my_materialized_view)

        assert table_count_end == view_count_end
        assert view_count_start != view_count_end
