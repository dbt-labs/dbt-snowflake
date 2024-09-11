import pytest

from pathlib import Path

from dbt.tests.util import run_dbt


_MODEL_BASIC_TABLE_MODEL = """
{{
  config(
    materialized = "table",
    cluster_by=['id'],
  )
}}
select 1 as id
""".strip()

_MODEL_BASIC_ICEBERG_MODEL = """
{{
  config(
    transient = "true",
    materialized = "table",
    cluster_by=['id'],
    table_format="iceberg",
    external_volume="s3_iceberg_snow",
    base_location_subpath="subpath",
  )
}}

select * from {{ ref('first_table') }}
""".strip()

_MODEL_BUILT_ON_ICEBERG_TABLE = """
{{
  config(
    materialized = "table",
  )
}}
select * from {{ ref('iceberg_table') }}
""".strip()

_MODEL_TABLE_FOR_SWAP = """
{{
  config(
    materialized = "table",
  )
}}
select 1 as id
""".strip()

_MODEL_TABLE_FOR_SWAP_ICEBERG = """
{{
  config(
    materialized = "table",
    table_format="iceberg",
    external_volume="s3_iceberg_snow",
    base_location_subpath="subpath",
  )
}}
select 1 as id
""".strip()


class TestIcebergTableBuilds:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "first_table.sql": _MODEL_BASIC_TABLE_MODEL,
            "iceberg_table.sql": _MODEL_BASIC_ICEBERG_MODEL,
            "table_built_on_iceberg_table.sql": _MODEL_BUILT_ON_ICEBERG_TABLE,
        }

    def test_iceberg_tables_build_and_can_be_referred(self, project):
        run_results = run_dbt()
        assert len(run_results) == 3


class TestIcebergTableTypeBuildsOnExistingTable:
    model_name = "my_model.sql"

    @pytest.fixture(scope="class")
    def models(self):
        return {self.model_name: _MODEL_TABLE_FOR_SWAP}

    def test_changing_model_types(self, project):
        model_file = project.project_root / Path("models") / Path(self.model_name)

        run_results = run_dbt()
        assert len(run_results) == 1

        rm_file(model_file)
        write_file(_MODEL_TABLE_FOR_SWAP_ICEBERG, model_file)
        run_results = run_dbt()
        assert len(run_results) == 1

        rm_file(model_file)
        write_file(_MODEL_TABLE_FOR_SWAP, model_file)
        run_results = run_dbt()
        assert len(run_results) == 1
