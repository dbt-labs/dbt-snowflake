import pytest
from dbt.tests.adapter.empty.test_empty import BaseTestEmpty, schema_sources_yml


class TestSnowflakeEmpty(BaseTestEmpty):
    pass


class TestSnowflakeEmptyInlineSourceRef(BaseTestEmpty):
    @pytest.fixture(scope="class")
    def models(self):
        model_sql = """
            select * from {{ source('seed_sources', 'raw_source') }} as raw_source
            """

        return {
            "model.sql": model_sql,
            "sources.yml": schema_sources_yml,
        }

    def test_run_with_empty(self, project):
        # create source from seed
        project.run_dbt(["seed"])

        project.run_dbt(["run", "--empty"])
        self.assert_row_count(project, "model", 0)
