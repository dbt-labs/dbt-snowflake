import pytest
from dbt.tests.adapter.unit_testing.test_unit_testing_types import BaseUnitTestingTypes


class TestBigQueryUnitTestingTypes(BaseUnitTestingTypes):
    @pytest.fixture
    def data_types(self):
        # sql_value, yaml_value
        return [
            ["1", "1"],
            ["'1'", "1"],
            ["cast('true' as boolean)", "true"],
            ["cast('2019-01-01' as date)", "2019-01-01"],
            ["cast('2013-11-03 00:00:00-07' as TIMESTAMP)", "2013-11-03 00:00:00-07"],
            ["['a','b','c']", "['a','b','c']"],
            ["[1,2,3]", "[1,2,3]"],
            ["cast(1 as NUMERIC)", "1"],
            ["""'{"name": "Cooper", "forname": "Alice"}'""", """'{"name": "Cooper", "forname": "Alice"}'"""],
        ]
