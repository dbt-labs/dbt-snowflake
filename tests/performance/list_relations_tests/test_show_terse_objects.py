from datetime import timedelta

import pytest

from tests.performance.list_relations_tests.list_relations import BaseConfig, Scenario


SHOW_TERSE_OBJECTS_MACRO = """
{% macro snowflake__get_show_objects_sql(schema, results_per_iteration) %}
    show terse objects in {{ schema.database }}.{{ schema.schema }} limit {{ results_per_iteration }}
{% endmacro %}
"""


class ShowTerseObjects(BaseConfig):
    @pytest.fixture(scope="class")
    def macros(self):
        yield {"snowflake__get_show_objects_sql.sql": SHOW_TERSE_OBJECTS_MACRO}


class TestShowTerseObjects10View10Table10Dynamic(ShowTerseObjects):
    scenario = Scenario(10, 10, 10)
    expected_duration = timedelta(seconds=1, microseconds=20_000).total_seconds()


class TestShowTerseObjects15View15Table0Dynamic(ShowTerseObjects):
    scenario = Scenario(15, 15, 0)
    expected_duration = timedelta(seconds=1, microseconds=20_000).total_seconds()


class TestShowTerseObjects100View100Table100Dynamic(ShowTerseObjects):
    scenario = Scenario(100, 100, 100)
    expected_duration = timedelta(seconds=0, microseconds=960_000).total_seconds()


class TestShowTerseObjects150View150Table0Dynamic(ShowTerseObjects):
    scenario = Scenario(150, 150, 0)
    expected_duration = timedelta(seconds=0, microseconds=960_000).total_seconds()


class TestShowTerseObjects1000View1000Table1000Dynamic(ShowTerseObjects):
    scenario = Scenario(1000, 1000, 1000)
    expected_duration = timedelta(seconds=2, microseconds=330_000).total_seconds()


class TestShowTerseObjects1500View1500Table0Dynamic(ShowTerseObjects):
    scenario = Scenario(1500, 1500, 0)
    expected_duration = timedelta(seconds=2, microseconds=330_000).total_seconds()
