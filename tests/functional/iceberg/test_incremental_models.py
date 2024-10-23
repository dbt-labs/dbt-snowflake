import pytest
import time

from pathlib import Path

from dbt.tests.util import run_dbt, run_dbt_and_capture, write_file


_SEED_INCREMENTAL_STRATEGIES = """
world_id,world_name,boss
1,Yoshi's Island,Iggy
2,Donut Plains,Morton
3,Vanilla Dome,Lemmy
4,Cookie Mountain,Temmy
5,Forest of Illusion,Roy
""".strip()

_MODEL_BASIC_TABLE_MODEL = """
{{
  config(
    materialized = "table",
  )
}}
select * from {{ ref('seed') }}
"""

_MODEL_INCREMENTAL_ICEBERG_BASE = """
{{{{
  config(
    materialized='incremental',
    table_format='iceberg',
    incremental_strategy='{strategy}',
    unique_key="world_id",
    external_volume = "s3_iceberg_snow",
    on_schema_change = "sync_all_columns"
  )
}}}}
select * from {{{{ ref('upstream_table') }}}}

{{% if is_incremental() %}}
where world_id > 2
{{% endif %}}
"""

_MODEL_INCREMENTAL_ICEBERG_APPEND = _MODEL_INCREMENTAL_ICEBERG_BASE.format(strategy="append")
_MODEL_INCREMENTAL_ICEBERG_MERGE = _MODEL_INCREMENTAL_ICEBERG_BASE.format(strategy="merge")
_MODEL_INCREMENTAL_ICEBERG_DELETE_INSERT = _MODEL_INCREMENTAL_ICEBERG_BASE.format(
    strategy="delete+insert"
)


_QUERY_UPDATE_UPSTREAM_TABLE = """
UPDATE {database}.{schema}.upstream_table set world_name = 'Twin Bridges', boss = 'Ludwig' where world_id = 4;
"""

_QUERY_UPDATE_UPSTREAM_TABLE_NO_EFFECT = """
UPDATE {database}.{schema}.upstream_table set world_name = 'Doughnut Plains' where world_id = 2;
"""


class TestIcebergIncrementalStrategies:
    append: str = f"append_{hash(time.time())}"

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"enable_iceberg_materializations": True}}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "seed.csv": _SEED_INCREMENTAL_STRATEGIES,
        }

    @pytest.fixture(scope="function", autouse=True)
    def setup_class(self, project):
        run_dbt(["seed"])
        yield

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "upstream_table.sql": _MODEL_BASIC_TABLE_MODEL,
            f"{self.append}.sql": _MODEL_INCREMENTAL_ICEBERG_APPEND,
            "merge.sql": _MODEL_INCREMENTAL_ICEBERG_MERGE,
            "delete_insert.sql": _MODEL_INCREMENTAL_ICEBERG_DELETE_INSERT,
        }

    def __check_correct_operations(self, model_name, /, rows_affected, status="SUCCESS"):
        run_results = run_dbt(
            ["show", "--inline", f"select * from {{{{ ref('{model_name}') }}}} where world_id = 4"]
        )
        assert run_results[0].adapter_response["rows_affected"] == rows_affected
        assert run_results[0].adapter_response["code"] == status

        if "append" not in model_name:
            run_results, stdout = run_dbt_and_capture(
                [
                    "show",
                    "--inline",
                    f"select * from {{{{ ref('{model_name}') }}}} where world_id = 2",
                ]
            )
            run_results[0].adapter_response["rows_affected"] == 1
            assert "Doughnut" not in stdout

    def test_incremental_strategies_with_update(self, project, setup_class):
        run_results = run_dbt()
        assert len(run_results) == 4

        project.run_sql(
            _QUERY_UPDATE_UPSTREAM_TABLE.format(
                database=project.database, schema=project.test_schema
            )
        )
        project.run_sql(
            _QUERY_UPDATE_UPSTREAM_TABLE_NO_EFFECT.format(
                database=project.database, schema=project.test_schema
            )
        )

        run_results = run_dbt(["run", "-s", self.append, "merge", "delete_insert"])
        assert len(run_results) == 3

        self.__check_correct_operations(self.append, rows_affected=2)
        self.__check_correct_operations("merge", rows_affected=1)
        self.__check_correct_operations("delete_insert", rows_affected=1)


class TestIcebergIncrementalOnSchemaChangeMutatesRelations:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"enable_iceberg_materializations": True}}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "seed.csv": _SEED_INCREMENTAL_STRATEGIES,
        }

    @pytest.fixture(scope="function", autouse=True)
    def setup_class(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])
        yield

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "upstream_table.sql": _MODEL_BASIC_TABLE_MODEL,
            "merge.sql": _MODEL_INCREMENTAL_ICEBERG_MERGE,
        }

    def test_sync_and_append_semantics(self, project, setup_class):
        model_file = project.project_root / Path("models") / Path("merge.sql")
        sql = f"show columns in {project.database}.{project.test_schema}.merge;"
        column_names = [column[2] for column in project.run_sql(sql, fetch="all")]
        assert len(column_names) == 3

        write_file(_MODEL_INCREMENTAL_ICEBERG_MERGE.replace("*", "*, 1 as new_column"), model_file)
        run_dbt()
        column_names = [column[2].lower() for column in project.run_sql(sql, fetch="all")]
        assert len(column_names) == 4
        assert "new_column" in column_names

        write_file(_MODEL_INCREMENTAL_ICEBERG_MERGE, model_file)
        run_dbt()
        column_names = [column[2].lower() for column in project.run_sql(sql, fetch="all")]
        assert len(column_names) == 3
        assert "new_column" not in column_names
