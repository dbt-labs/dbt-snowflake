import pytest
from dbt.tests.util import run_dbt


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


models__models_config_yml = """
version: 2

models:
  - name: view_model_query_tag
    columns:
      - name: id
        data_tests:
          - unique
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
""".strip()


class TestQueryTag:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table_model_query_tag.sql": models__table_model_query_tag_sql,
            "view_model_query_tag.sql": models__view_model_query_tag_sql,
            "incremental_model_query_tag.sql": models__incremental_model_query_tag_sql,
            "models_config.yml": models__models_config_yml,
        }

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snapshot_query_tag.sql": snapshots__snapshot_query_tag_sql}

    @pytest.fixture(scope="class")
    def macros(self):
        return {"check_tag.sql": macros__check_tag_sql}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed_query_tag.csv": seeds__seed_query_tag_csv}

    @pytest.fixture(scope="class")
    def project_config_update(self, prefix):
        return {
            "config-version": 2,
            "models": {"query_tag": prefix, "post-hook": "{{ check_query_tag() }}"},
            "seeds": {"query_tag": prefix, "post-hook": "{{ check_query_tag() }}"},
            "snapshots": {"query_tag": prefix, "post-hook": "{{ check_query_tag() }}"},
            "tests": {"test": {"query_tag": prefix, "post-hook": "{{ check_query_tag() }}"}},
        }

    def build_all_with_query_tags(self, project, prefix):
        run_dbt(["build", "--vars", '{{"query_tag": "{}"}}'.format(prefix)])

    def test_snowflake_query_tag(self, project, prefix):
        self.build_all_with_query_tags(project, prefix)
        self.build_all_with_query_tags(project, prefix)


class TestSnowflakeProfileQueryTag:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table_model_query_tag.sql": models__table_model_query_tag_sql,
            "view_model_query_tag.sql": models__view_model_query_tag_sql,
            "incremental_model_query_tag.sql": models__incremental_model_query_tag_sql,
            "models_config.yml": models__models_config_yml,
        }

    @pytest.fixture(scope="class")
    def profiles_config_update(self, prefix):
        return {"query_tag": prefix}

    def build_all_with_query_tags(self, project, prefix):
        run_dbt(["build", "--vars", '{{"query_tag": "{}"}}'.format(prefix)])

    def test_snowflake_query_tag(self, project, prefix):
        self.build_all_with_query_tags(project, prefix)
        self.build_all_with_query_tags(project, prefix)
