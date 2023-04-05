import pytest

from dbt.tests.adapter.constraints.test_constraints import (
    BaseTableConstraintsColumnsEqual,
    BaseViewConstraintsColumnsEqual,
    BaseIncrementalConstraintsColumnsEqual,
    BaseConstraintsRuntimeDdlEnforcement,
    BaseConstraintsRollback,
    BaseIncrementalConstraintsRuntimeDdlEnforcement,
    BaseIncrementalConstraintsRollback, BaseModelConstraintsRuntimeEnforcement,
)


_expected_sql_snowflake = """
create or replace transient table <model_identifier> (
    id integer not null primary key,
    color text,
    date_day text
) as ( select
        id,
        color,
        date_day from
    (
    select
        'blue' as color,
        1 as id,
        '2019-01-01' as date_day
    ) as model_subq
);
"""


class SnowflakeColumnEqualSetup:
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


class TestSnowflakeTableConstraintsColumnsEqual(SnowflakeColumnEqualSetup, BaseTableConstraintsColumnsEqual):
    pass


class TestSnowflakeViewConstraintsColumnsEqual(SnowflakeColumnEqualSetup, BaseViewConstraintsColumnsEqual):
    pass


class TestSnowflakeIncrementalConstraintsColumnsEqual(SnowflakeColumnEqualSetup, BaseIncrementalConstraintsColumnsEqual):
    pass


class TestSnowflakeTableConstraintsDdlEnforcement(BaseConstraintsRuntimeDdlEnforcement):
    @pytest.fixture(scope="class")
    def expected_sql(self):
        return _expected_sql_snowflake


class TestSnowflakeIncrementalConstraintsDdlEnforcement(BaseIncrementalConstraintsRuntimeDdlEnforcement):
    @pytest.fixture(scope="class")
    def expected_sql(self):
        return _expected_sql_snowflake


class TestSnowflakeTableConstraintsRollback(BaseConstraintsRollback):
    @pytest.fixture(scope="class")
    def expected_error_messages(self):
        return ["NULL result in a non-nullable column"]


class TestSnowflakeIncrementalConstraintsRollback(BaseIncrementalConstraintsRollback):
    @pytest.fixture(scope="class")
    def expected_error_messages(self):
        return ["NULL result in a non-nullable column"]


class TestSnowflakeModelConstraintsRuntimeEnforcement(BaseModelConstraintsRuntimeEnforcement):
    @pytest.fixture(scope="class")
    def expected_sql(self):
        return """
create or replace transient table <model_identifier> (
    id integer not null,
    color text,
    date_day text,
    primary key (id),
    constraint strange_uniqueness_requirement unique (color, date_day)
) as ( select
        id,
        color,
        date_day from
    (
    select
        1 as id,
        'blue' as color,
        '2019-01-01' as date_day
    ) as model_subq
);
"""