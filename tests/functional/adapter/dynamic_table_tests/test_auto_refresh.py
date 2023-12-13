from datetime import datetime

import pytest

# it's the same test for DTs as for MVs in other adapters
from dbt.tests.adapter.materialized_view.auto_refresh import (
    MaterializedViewAutoRefreshNoChanges,
)

from tests.functional.adapter.dynamic_table_tests import files


class TestDynamicTableAutoRefreshNoChanges(MaterializedViewAutoRefreshNoChanges):
    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": files.MY_SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "auto_refresh_on.sql": files.MY_DYNAMIC_TABLE,
        }

    @pytest.fixture(scope="class", autouse=True)
    def macros(self):
        yield {"snowflake__test__last_refresh.sql": files.MACRO__LAST_REFRESH}

    def last_refreshed(self, project, dynamic_table: str) -> datetime:
        with project.adapter.connection_named("__test"):
            kwargs = {"schema": project.test_schema, "identifier": dynamic_table}
            last_refresh_results = project.adapter.execute_macro(
                "snowflake__test__last_refresh", kwargs=kwargs
            )
        last_refresh = last_refresh_results[0].get("last_refresh")
        return last_refresh

    @pytest.mark.skip("Snowflake does not support turning off auto refresh.")
    def test_manual_refresh_occurs_when_auto_refresh_is_off(self, project):
        pass
