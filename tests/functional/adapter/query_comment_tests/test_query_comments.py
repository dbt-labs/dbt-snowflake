import pytest
from dbt.tests.adapter.query_comment.test_query_comment import (
    BaseQueryComments,
    BaseMacroQueryComments,
    BaseMacroArgsQueryComments,
    BaseMacroInvalidQueryComments,
    BaseNullQueryComments,
    BaseEmptyQueryComments,
)


class TestQueryCommentsSnowflake(BaseQueryComments):
    pass

class TestMacroQueryCommentsSnowflake(BaseMacroQueryComments):
    pass

class TestMacroArgsQueryCommentsSnowflake(BaseMacroArgsQueryComments):
    pass

class TestMacroInvalidQueryCommentsSnowflake(BaseMacroInvalidQueryComments):
    pass

class TestNullQueryCommentsSnowflake(BaseNullQueryComments):
    pass

class TestEmptyQueryCommentsSnowflake(BaseEmptyQueryComments):
    pass