import pytest

from dbt.tests.adapter.unit_testing.test_types import BaseUnitTestingTypes
from dbt.tests.adapter.unit_testing.test_case_insensitivity import BaseUnitTestCaseInsensivity
from dbt.tests.adapter.unit_testing.test_invalid_input import BaseUnitTestInvalidInput


class TestSnowflakeUnitTestingTypes(BaseUnitTestingTypes):
    @pytest.fixture
    def data_types(self):
        # sql_value, yaml_value
        return [
            ["1", "1"],
            ["2.0", "2.0"],
            ["'12345'", "12345"],
            ["'string'", "string"],
            ["true", "true"],
            ["DATE '2020-01-02'", "2020-01-02"],
            ["TIMESTAMP '2013-11-03 00:00:00-0'", "2013-11-03 00:00:00-0"],
            ["'2013-11-03 00:00:00-0'::TIMESTAMPTZ", "2013-11-03 00:00:00-0"],
            ["TO_NUMBER('3', 10, 9)", "3"],
            ["3::VARIANT", "3"],
            ["TO_GEOMETRY('POINT(1820.12 890.56)')", "POINT(1820.12 890.56)"],
            ["TO_GEOGRAPHY('POINT(-122.35 37.55)')", "POINT(-122.35 37.55)"],
            [
                "{'Alberta':'Edmonton','Manitoba':'Winnipeg'}",
                "{'Alberta':'Edmonton','Manitoba':'Winnipeg'}",
            ],
            ["['a','b','c']", "['a','b','c']"],
            ["[1,2,3]", "[1, 2, 3]"],
        ]


class TestSnowflakeUnitTestCaseInsensitivity(BaseUnitTestCaseInsensivity):
    pass


class TestSnowflakeUnitTestInvalidInput(BaseUnitTestInvalidInput):
    pass
