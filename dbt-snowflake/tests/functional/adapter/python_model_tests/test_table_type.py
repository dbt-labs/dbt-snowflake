import pytest

from dbt.tests.util import run_dbt

from tests.functional.adapter.python_model_tests import _files


class TestTableType:
    @pytest.fixture(scope="class")
    def macros(self):
        return {"snowflake__test__describe_tables.sql": _files.MACRO__DESCRIBE_TABLES}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            # <temporary>_<transient>_table
            "TRANSIENT_TRUE_TABLE.py": _files.TRANSIENT_TRUE_TABLE,
            "TRANSIENT_FALSE_TABLE.py": _files.TRANSIENT_FALSE_TABLE,
            "TRANSIENT_NONE_TABLE.py": _files.TRANSIENT_NONE_TABLE,
            "TRANSIENT_UNSET_TABLE.py": _files.TRANSIENT_UNSET_TABLE,
        }

    def test_expected_table_types_are_created(self, project):
        run_dbt(["run"])
        expected_table_types = {
            # (name, kind) - TABLE == permanent
            ("TRANSIENT_TRUE_TABLE", "TRANSIENT"),
            ("TRANSIENT_FALSE_TABLE", "TABLE"),
            ("TRANSIENT_NONE_TABLE", "TABLE"),
            ("TRANSIENT_UNSET_TABLE", "TRANSIENT"),
        }
        with project.adapter.connection_named("__test"):
            agate_table = project.adapter.execute_macro("snowflake__test__describe_tables")
        actual_table_types = {(row.get("name"), row.get("kind")) for row in agate_table.rows}
        assert actual_table_types == expected_table_types
