import pytest
from dbt.tests.util import (
    run_dbt,
    get_manifest,
    run_dbt_and_capture
)
from dbt.tests.adapter.constraints.test_constraints import (
  BaseConstraintsColumnsEqual,
  BaseConstraintsRuntimeEnforcement
)

_expected_sql_snowflake = """
create or replace transient table {0}.{1}.my_model (
    id integer not null primary key ,
    color text ,
    date_day date
) as (
    select 1 as id,
    'blue' as color,
    cast('2019-01-01' as date) as date_day
);
"""

class TestSnowflakeConstraintsColumnsEqual(BaseConstraintsColumnsEqual):
    pass


class TestSnowflakeConstraintsRuntimeEnforcement(BaseConstraintsRuntimeEnforcement):
    @pytest.fixture(scope="class")
    def expected_sql(self, project):
        return _expected_sql_snowflake.format(project.database, project.test_schema)

    @pytest.fixture(scope="class")
    def expected_error_messages(self):
        return ['NULL result in a non-nullable column']
