from dbt.tests.adapter.dbt_show.test_dbt_show import (
    BaseShowSqlHeader,
    BaseShowLimit,
    BaseShowDoesNotHandleDoubleLimit,
)


class TestSnowflakeShowLimit(BaseShowLimit):
    pass


class TestSnowflakeShowSqlHeader(BaseShowSqlHeader):
    pass


class TestSnowflakeShowDoesNotHandleDoubleLimit(BaseShowDoesNotHandleDoubleLimit):
    DATABASE_ERROR_MESSAGE = "unexpected 'limit'"
