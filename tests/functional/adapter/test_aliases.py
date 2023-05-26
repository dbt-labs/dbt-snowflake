import pytest
from dbt.tests.adapter.aliases.test_aliases import BaseAliases

MACROS__SNOWFLAKE_CAST_SQL = """
{% macro snowflake__string_literal(s) %}
    cast('{{ s }}' as string)
{% endmacro %}
"""

MACROS__EXPECT_VALUE_SQL = """
-- cross-db compatible test, similar to accepted_values

{% test expect_value(model, field, value) %}

select *
from {{ model }}
where {{ field }} != '{{ value }}'

{% endtest %}
"""


class TestAliasesSnowflake(BaseAliases):
    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "snowflake_cast.sql": MACROS__SNOWFLAKE_CAST_SQL,
            "expect_value.sql": MACROS__EXPECT_VALUE_SQL,
        }
