import pytest

from dbt.tests.util import relation_from_name
from dbt.tests.adapter.constraints.test_constraints import (
    BaseTableConstraintsColumnsEqual,
    BaseViewConstraintsColumnsEqual,
    BaseConstraintsRuntimeEnforcement
)


_expected_sql_snowflake = """
create or replace transient table {0} (
    id integer not null primary key ,
    color text ,
    date_day date
) as (
    select
        1 as id,
        'blue' as color,
        cast('2019-01-01' as date) as date_day
);
"""


class SnowflakeSetup:
    @pytest.fixture
    def int_type(self):
        return "FIXED"

    @pytest.fixture
    def schema_int_type(self):
        return "INT"

    @pytest.fixture
    def data_types(self, int_type, schema_int_type, string_type):
        # sql_column_value, schema_data_type, error_data_type
        return [
            ['1', schema_int_type, int_type],
            ["'1'", string_type, string_type],
            ["cast('2019-01-01' as date)", 'date', 'DATE'],
            ["true", 'boolean', 'BOOLEAN'],
            ["'2013-11-03 00:00:00-07'::timestamptz", 'timestamp_tz', 'TIMESTAMP_TZ'],
            ["'2013-11-03 00:00:00-07'::timestamp", 'timestamp', 'TIMESTAMP_NTZ'],
            ["ARRAY_CONSTRUCT('a','b','c')", 'array', 'ARRAY'],
            ["ARRAY_CONSTRUCT(1,2,3)", 'array', 'ARRAY'],
            ["""TO_VARIANT(PARSE_JSON('{"key3": "value3", "key4": "value4"}'))""", 'variant', 'VARIANT'],
        ]

class TestSnowflakeTableConstraintsColumnsEqual(SnowflakeSetup, BaseTableConstraintsColumnsEqual):
    pass


class TestSnowflakeViewConstraintsColumnsEqual(SnowflakeSetup, BaseViewConstraintsColumnsEqual):
    pass


class TestSnowflakeConstraintsRuntimeEnforcement(BaseConstraintsRuntimeEnforcement):

    @pytest.fixture(scope="class")
    def expected_sql(self, project):
        relation = relation_from_name(project.adapter, "my_model")
        return _expected_sql_snowflake.format(relation)

    @pytest.fixture(scope="class")
    def expected_error_messages(self):
        return ["NULL result in a non-nullable column"]
