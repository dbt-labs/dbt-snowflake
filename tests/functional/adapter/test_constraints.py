import pytest
from dbt.tests.util import relation_from_name
from dbt.tests.adapter.constraints.test_constraints import (
    BaseConstraintsColumnsEqual,
    BaseConstraintsRuntimeEnforcement
)
from dbt.tests.adapter.constraints.fixtures import (
    my_model_wrong_order_sql,
    my_model_wrong_name_sql,
    model_schema_yml,
    my_model_wrong_data_type_sql,
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

# Different on Snowflake:
# - does not support a data type named 'int[]'
constraints_yml = model_schema_yml.replace("int[]", "ARRAY")
# - SELECT ARRAY[1,2,3]; is invalid. SELECT [1,2,3] works.
my_model_wrong_data_type_sql = my_model_wrong_data_type_sql.replace("ARRAY", "")


class TestSnowflakeConstraintsColumnsEqual(BaseConstraintsColumnsEqual):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_wrong_order.sql": my_model_wrong_order_sql,
            "my_model_wrong_name.sql": my_model_wrong_name_sql,
            "my_model_wrong_data_type.sql": my_model_wrong_data_type_sql,
            "constraints_schema.yml": constraints_yml,
        }

    @pytest.fixture
    def int_type(self):
        return "FIXED"

    @pytest.fixture
    def int_array_type(self):
        return "ARRAY"

    @pytest.fixture
    def string_array_type(self):
        return "ARRAY"


class TestSnowflakeConstraintsRuntimeEnforcement(BaseConstraintsRuntimeEnforcement):
    @pytest.fixture(scope="class")
    def expected_sql(self, project):
        relation = relation_from_name(project.adapter, "my_model")
        return _expected_sql_snowflake.format(relation)

    @pytest.fixture(scope="class")
    def expected_error_messages(self):
        return ["NULL result in a non-nullable column"]
