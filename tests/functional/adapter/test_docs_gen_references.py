import pytest
from dbt.tests.adapter.basic.test_docs_generate import (
    BaseDocsGenReferences, ref_models__ephemeral_copy_sql, ref_models__docs_md
)
from dbt.tests.adapter.basic.expected_catalog import expected_references_catalog, no_stats

ref_models__schema_yml = """
version: 2
models:
  - name: ephemeral_summary
    description: "{{ doc('ephemeral_summary') }}"
    columns: &summary_columns
      - name: first_name
        description: "{{ doc('summary_first_name') }}"
      - name: ct
        description: "{{ doc('summary_count') }}"
  - name: view_summary
    description: "{{ doc('view_summary') }}"
    columns: *summary_columns
exposures:
  - name: notebook_exposure
    type: notebook
    depends_on:
      - ref('view_summary')
    owner:
      email: something@example.com
      name: Some name
    description: "{{ doc('notebook_info') }}"
    maturity: medium
    url: http://example.com/notebook/1
    meta:
      tool: 'my_tool'
      languages:
        - python
    tags: ['my_department']
"""

ref_sources__schema_yml = """
version: 2
sources:
  - name: my_source
    description: "{{ doc('source_info') }}"
    loader: a_loader
    schema: "{{ var('test_schema') }}"
    quoting:
      database: False
      identifier: False
    tables:
      - name: my_table
        description: "{{ doc('table_info') }}"
        identifier: SEED
        quoting:
          identifier: True
        columns:
          - name: ID
            description: "{{ doc('column_info') }}"
"""

ref_models__view_summary_sql = """
{{
  config(
    materialized = "view"
  )
}}
select first_name, ct from {{ref('ephemeral_summary')}}
order by CT asc
"""

ref_models__ephemeral_summary_sql = """
{{
  config(
    materialized = "table"
  )
}}
select first_name, count(*) as CT from {{ref('ephemeral_copy')}}
group by first_name
order by first_name asc
"""

class TestReferencesSnowflake(BaseDocsGenReferences):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": ref_models__schema_yml,
            "sources.yml": ref_sources__schema_yml,
            "view_summary.sql": ref_models__view_summary_sql,
            "ephemeral_summary.sql": ref_models__ephemeral_summary_sql,
            "ephemeral_copy.sql": ref_models__ephemeral_copy_sql,
            "docs.md": ref_models__docs_md,
        }

    @pytest.fixture(scope="class")
    def get_role(self, project):
        return project.run_sql('select current_role()', fetch='one')[0]

    @pytest.fixture(scope="class")
    def expected_catalog(self, project, get_role):
        return expected_references_catalog(
            project,
            role=get_role,
            id_type="NUMBER",
            text_type="TEXT",
            time_type="TIMESTAMP_NTZ",
            bigint_type="BIGINT",
            view_type="VIEW",
            table_type="BASE TABLE",
            model_stats=no_stats(),
            case=lambda x: x.upper(),
            case_columns=False,
        )
