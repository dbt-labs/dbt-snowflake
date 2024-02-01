import pytest

from dbt.tests.adapter.unit_testing.test_unit_testing import BaseUnitTestingTypes


class TestSnowflakeUnitTestingTypes(BaseUnitTestingTypes):
    @pytest.fixture
    def data_types(self):
        # sql_value, yaml_value
        return [
            ["1", "1"],
            ["'12345'", "12345"],
            ["'string'", "string"],
            ["true", "true"],
            ["DATE '2020-01-02'", "2020-01-02"],
            ["TIMESTAMP '2013-11-03 00:00:00-0'", "2013-11-03 00:00:00-0"],
            ["'2013-11-03 00:00:00-0'::TIMESTAMPTZ", "2013-11-03 00:00:00-0"],
            ["TO_NUMBER('3', 10, 9)", "3"],
            # ["3::VARIANT", "3"],
            # [
            #     """TO_JSON(PARSE_JSON('{"bar": "baz", "balance": 7.77, "active": false}'))""",
            #     """'TO_JSON(PARSE_JSON('{"bar": "baz", "balance": 7.77, "active": false}'))'""",
            # ],
            # TODO: support complex types
            # ["ARRAY['a','b','c']", """'{"a", "b", "c"}'"""],
            # ["ARRAY[1,2,3]", """'{1, 2, 3}'"""],
        ]
