import pytest
from dbt.tests.util import run_dbt
import os

snapshots__snapshot_query_tag_sql = """
{% snapshot snapshot_query_tag %}
    {{
        config(
            target_database=database,
            target_schema=schema,
            unique_key='id',
            strategy='check',
            check_cols=['color'],
        )
    }}
    select 1 as id, 'blue' as color
{% endsnapshot %}

"""

models__table_model_query_tag_sql = """
{{ config(materialized = 'table') }}

select 1 as id

"""

models__view_model_query_tag_sql = """
{{ config(materialized = 'view') }}

select 1 as id

"""

models__incremental_model_query_tag_sql = """
{{ config(materialized = 'incremental', unique_key = 'id') }}

select 1 as id

"""

macros__check_tag_sql = """
{% macro check_query_tag() %}

  {% if execute %}
    {% set query_tag = get_current_query_tag() %}
    {% if query_tag != var("query_tag") %}
      {{ exceptions.raise_compiler_error("Query tag not used!") }}
    {% endif %}
  {% endif %}

{% endmacro %}

"""

seeds__seed_query_tag_csv = """id
1
"""

class TestQueryTag:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table_model_query_tag.sql": models__table_model_query_tag_sql,
            "view_model_query_tag.sql": models__view_model_query_tag_sql,
            "incremental_model_query_tag.sql": models__incremental_model_query_tag_sql
            }

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {
            "snapshot_query_tag.sql": snapshots__snapshot_query_tag_sql
            }

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "check_tag.sql": macros__check_tag_sql
            }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "seed_query_tag.csv": seeds__seed_query_tag_csv
            }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            'config-version': 2,
            'models': {
                'tests': {
                    'query_tag': self.prefix,
                    'post-hook': '{{ check_tag() }}'
                },
            },
            'seeds': {
                'tests': {
                    'query_tag': self.prefix,
                    'post-hook': '{{ check_tag() }}'
                },
            },
            'snapshots': {
                'tests': {
                    'query_tag': self.prefix,
                    'post-hook': '{{ check_tag() }}'
                },
            },
        }

    def build_all_with_query_tags(self, project):
        run_dbt(['build', '--vars', '{{"check_tag": "{}"}}'.format(self.prefix)])

    def test_snowflake_query_tag(self, project):
        self.build_all_with_query_tags(project)
        self.build_all_with_query_tags(project)




