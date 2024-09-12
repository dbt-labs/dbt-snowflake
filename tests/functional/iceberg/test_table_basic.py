import pytest

from pathlib import Path

from dbt.tests.util import run_dbt, rm_file, write_file

_MODEL_BASIC_TABLE_MODEL = """
{{
  config(
    materialized = "table",
    cluster_by=['id'],
  )
}}
select 1 as id
"""

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
"""

_MODEL_BUILT_ON_ICEBERG_TABLE = """
{{
  config(
    materialized = "table",
  )
}}
select * from {{ ref('iceberg_table') }}
"""

_MODEL_TABLE_BEFORE_SWAP = """
{{
  config(
    materialized = "table",
  )
}}
select 1 as id
"""

_MODEL_VIEW_BEFORE_SWAP = """
select 1 as id
"""

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
"""


class TestIcebergTableBuilds:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"enable_iceberg_materializations": True}}

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
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"enable_iceberg_materializations": True}}

    @pytest.mark.parametrize("start_model", [_MODEL_TABLE_BEFORE_SWAP, _MODEL_VIEW_BEFORE_SWAP])
    def test_changing_model_types(self, project, start_model):
        model_file = project.project_root / Path("models") / Path("my_model.sql")

        write_file(start_model, model_file)
        run_results = run_dbt()
        assert len(run_results) == 1

        rm_file(model_file)
        write_file(_MODEL_TABLE_FOR_SWAP_ICEBERG, model_file)
        run_results = run_dbt()
        assert len(run_results) == 1

        rm_file(model_file)
        write_file(start_model, model_file)
        run_results = run_dbt()
        assert len(run_results) == 1
