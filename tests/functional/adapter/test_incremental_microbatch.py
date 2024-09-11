import pytest
from dbt.tests.adapter.incremental.test_incremental_microbatch import (
    BaseMicrobatch,
)


class TestSnowflakeMicrobatch(BaseMicrobatch):
    @pytest.fixture(scope="class")
    def insert_two_rows_sql(self, project) -> str:
        test_schema_relation = project.adapter.Relation.create(
            database=project.database, schema=project.test_schema
        )
        return f"insert into {test_schema_relation}.input_model (id, event_time) values (4, '2020-01-04 00:00:00-0'), (5, '2020-01-05 00:00:00-0')"
