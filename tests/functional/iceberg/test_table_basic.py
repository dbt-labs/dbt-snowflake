import pytest

from pathlib import Path

from dbt.tests.util import run_dbt, rm_file, write_file

from tests.functional.iceberg.models import (
    _MODEL_BASIC_TABLE_MODEL,
    _MODEL_BASIC_ICEBERG_MODEL,
    _MODEL_BASIC_ICEBERG_MODEL_WITH_PATH,
    _MODEL_BASIC_ICEBERG_MODEL_WITH_PATH_SUBPATH,
    _MODEL_BASIC_DYNAMIC_TABLE_MODEL,
    _MODEL_BASIC_DYNAMIC_TABLE_MODEL_WITH_PATH,
    _MODEL_BASIC_DYNAMIC_TABLE_MODEL_WITH_PATH_SUBPATH,
    _MODEL_BASIC_DYNAMIC_TABLE_MODEL_WITH_SUBPATH,
    _MODEL_BUILT_ON_ICEBERG_TABLE,
    _MODEL_TABLE_BEFORE_SWAP,
    _MODEL_VIEW_BEFORE_SWAP,
    _MODEL_TABLE_FOR_SWAP_ICEBERG,
)


class TestIcebergTableBuilds:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"enable_iceberg_materializations": True}}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "first_table.sql": _MODEL_BASIC_TABLE_MODEL,
            "iceberg_table.sql": _MODEL_BASIC_ICEBERG_MODEL,
            "iceberg_tableb.sql": _MODEL_BASIC_ICEBERG_MODEL_WITH_PATH,
            "iceberg_tablec.sql": _MODEL_BASIC_ICEBERG_MODEL_WITH_PATH_SUBPATH,
            "table_built_on_iceberg_table.sql": _MODEL_BUILT_ON_ICEBERG_TABLE,
            "dynamic_table.sql": _MODEL_BASIC_DYNAMIC_TABLE_MODEL,
            "dynamic_tableb.sql": _MODEL_BASIC_DYNAMIC_TABLE_MODEL_WITH_PATH,
            "dynamic_tablec.sql": _MODEL_BASIC_DYNAMIC_TABLE_MODEL_WITH_PATH_SUBPATH,
            "dynamic_tabled.sql": _MODEL_BASIC_DYNAMIC_TABLE_MODEL_WITH_SUBPATH,
        }

    def test_iceberg_tables_build_and_can_be_referred(self, project):
        run_results = run_dbt()
        assert len(run_results) == 9


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
