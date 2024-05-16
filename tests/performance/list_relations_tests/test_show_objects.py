from datetime import timedelta

import pytest

from tests.performance.list_relations_tests.list_relations import BaseConfig, Scenario


SHOW_OBJECTS_MACRO = """
{% macro snowflake__get_show_objects_sql(schema, results_per_iteration) %}
    show objects in {{ schema.database }}.{{ schema.schema }} limit {{ results_per_iteration }}
{% endmacro %}
"""


class ShowObjects(BaseConfig):
    @pytest.fixture(scope="class")
    def macros(self):
        yield {"snowflake__get_show_objects_sql.sql": SHOW_OBJECTS_MACRO}


class TestShowObjects10View10Table10Dynamic(ShowObjects):
    scenario = Scenario(10, 10, 10)
    expected_duration = timedelta(seconds=0, microseconds=920_000).total_seconds()


class TestShowObjects15View15Table0Dynamic(ShowObjects):
    scenario = Scenario(15, 15, 0)
    expected_duration = timedelta(seconds=0, microseconds=920_000).total_seconds()


class TestShowObjects100View100Table100Dynamic(ShowObjects):
    scenario = Scenario(100, 100, 100)
    expected_duration = timedelta(seconds=1, microseconds=370_000).total_seconds()


class TestShowObjects150View150Table0Dynamic(ShowObjects):
    scenario = Scenario(150, 150, 0)
    expected_duration = timedelta(seconds=1, microseconds=370_000).total_seconds()


class TestShowObjects1000View1000Table1000Dynamic(ShowObjects):
    scenario = Scenario(1000, 1000, 1000)
    expected_duration = timedelta(seconds=3, microseconds=400_000).total_seconds()


class TestShowObjects1500View1500Table0Dynamic(ShowObjects):
    scenario = Scenario(1500, 1500, 0)
    expected_duration = timedelta(seconds=3, microseconds=400_000).total_seconds()
