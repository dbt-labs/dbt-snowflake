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
    @pytest.mark.skip(
        "This test is incorrectly comparing the version of `dbt-core`"
        "to the version of `dbt-snowflake`, which is not always the same."
    )
    def test_matches_comment(self, project, get_package_version):
        pass


class TestMacroInvalidQueryCommentsSnowflake(BaseMacroInvalidQueryComments):
    pass


class TestNullQueryCommentsSnowflake(BaseNullQueryComments):
    pass


class TestEmptyQueryCommentsSnowflake(BaseEmptyQueryComments):
    pass
