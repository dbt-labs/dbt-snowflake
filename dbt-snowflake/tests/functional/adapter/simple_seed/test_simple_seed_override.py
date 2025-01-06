import pytest
from dbt.tests.adapter.simple_seed.test_seed_type_override import BaseSimpleSeedColumnOverride
from dbt.tests.adapter.utils.base_utils import run_dbt

_SCHEMA_YML = """
version: 2
seeds:
- name: seed_enabled
  columns:
  - name: birthday
    data_tests:
    - column_type:
        type: character varying(16777216)
  - name: seed_id
    data_tests:
    - column_type:
        type: FLOAT

- name: seed_tricky
  columns:
  - name: seed_id
    data_tests:
    - column_type:
        type: NUMBER(38,0)
  - name: seed_id_str
    data_tests:
    - column_type:
        type: character varying(16777216)
  - name: a_bool
    data_tests:
    - column_type:
        type: BOOLEAN
  - name: looks_like_a_bool
    data_tests:
    - column_type:
        type: character varying(16777216)
  - name: a_date
    data_tests:
    - column_type:
        type: TIMESTAMP_NTZ
  - name: looks_like_a_date
    data_tests:
    - column_type:
        type: character varying(16777216)
  - name: relative
    data_tests:
    - column_type:
        type: character varying(16777216)
  - name: weekday
    data_tests:
    - column_type:
        type: character varying(16777216)
""".lstrip()


class TestSimpleSeedColumnOverride(BaseSimpleSeedColumnOverride):
    @pytest.fixture(scope="class")
    def schema(self):
        return "simple_seed"

    @pytest.fixture(scope="class")
    def models(self):
        return {"models-snowflake.yml": _SCHEMA_YML}

    @staticmethod
    def seed_enabled_types():
        return {
            "seed_id": "FLOAT",
            "birthday": "TEXT",
        }

    @staticmethod
    def seed_tricky_types():
        return {
            "seed_id_str": "TEXT",
            "looks_like_a_bool": "TEXT",
            "looks_like_a_date": "TEXT",
        }

    def test_snowflake_simple_seed_with_column_override_snowflake(self, project):
        seed_results = run_dbt(["seed"])
        assert len(seed_results) == 2
        test_results = run_dbt(["test"])
        assert len(test_results) == 10
