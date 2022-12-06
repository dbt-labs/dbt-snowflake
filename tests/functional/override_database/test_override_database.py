import pytest
import os
from dbt.tests.util import (
    run_dbt,
    check_relations_equal_with_relations
)

_MODELS__VIEW_1_SQL = """
{#
	We are running against a database that must be quoted.
	These calls ensure that we trigger an error if we're failing to quote at parse-time
#}
{% do adapter.already_exists(this.schema, this.table) %}
{% do adapter.get_relation(this.database, this.schema, this.table) %}
select * from {{ ref('seed') }}
"""

_MODELS__VIEW_2_SQL = """
{%- if target.type == 'bigquery' -%}
  {{ config(project=var('alternate_db')) }}
{%- else -%}
  {{ config(database=var('alternate_db')) }}
{%- endif -%}
select * from {{ ref('seed') }}
"""

_MODELS__SUBFOLDER__VIEW_3_SQL = """
select * from {{ ref('seed') }}
"""

_MODELS__SUBFOLDER__VIEW_4_SQL = """
{{
    config(database=var('alternate_db'))
}}

select * from {{ ref('seed') }}
"""

_SEEDS__SEED_CSV = """id,name
1,a
2,b
3,c
4,d
5,e
"""

ALT_DATABASE = os.getenv("SNOWFLAKE_TEST_ALT_DATABASE")


class BaseOverrideDatabaseSnowflake:
  @pytest.fixture(scope="class")
  def seeds(self):
    return {"seed.csv": _SEEDS__SEED_CSV}

  @pytest.fixture(scope="class")
  def models(self):
        return {
          "view_1.sql": _MODELS__VIEW_1_SQL,
          "view_2.sql": _MODELS__VIEW_2_SQL,
          "view_3.sql": _MODELS__SUBFOLDER__VIEW_3_SQL,
          "view_4.sql": _MODELS__SUBFOLDER__VIEW_4_SQL,
        }

  @pytest.fixture(scope="class")
  def project_config_update(self):
        return {
            "config-version": 2,
            "seed-paths": ["seeds"],
            "vars": {
                "alternate_db": ALT_DATABASE,
            },
            "quoting": {
                "database": True,
            },
            "seeds": {
                "quote_columns": False,
            }
        }

  @pytest.fixture(scope="class")
  def clean_up(self, project):
    yield
    with project.adapter.connection_named('__test'):
        relation = project.adapter.Relation.create(
            database=ALT_DATABASE,
            schema=project.test_schema
        )
        project.adapter.drop_schema(relation)

  def check_caps(self, project, name):
    if project.adapter == "snowflake":
        return name.upper()
    else:
        return name


class TestModelOverrideSnowflake(BaseOverrideDatabaseSnowflake):
  def run_database_override(self, project):
    run_dbt(["seed"])
    result = run_dbt(["run"])
    assert len(result) == 4
    check_relations_equal_with_relations(project.adapter, [
              project.adapter.Relation.create(
                schema=project.test_schema,
                identifier=self.check_caps(project, "seed")
            ),
              project.adapter.Relation.create(
                schema=project.test_schema,
                identifier=self.check_caps(project, "view_1")
            ),
              project.adapter.Relation.create(
                database=ALT_DATABASE,
                schema=project.test_schema,
                identifier=self.check_caps(project, "view_2")
            ),
              project.adapter.Relation.create(
                schema=project.test_schema,
                identifier=self.check_caps(project, "view_3")
            ),
              project.adapter.Relation.create(
                database=ALT_DATABASE,
                schema=project.test_schema,
                identifier=self.check_caps(project, "view_4")
            )
          ])

  def test_snowflake_database_override(self, project, clean_up):
    self.run_database_override(project)


class TestProjectSeedOverrideSnowflake(BaseOverrideDatabaseSnowflake):
  @pytest.fixture(scope="class")
  def project_config_update(self):
      return {
            "config-version": 2,
            "seed-paths": ["seeds"],
            "vars": {
                "alternate_db": ALT_DATABASE,
            },
            "seeds": {
                "database": ALT_DATABASE
            }
        }

  def run_database_override(self, project):
      run_dbt(["seed"])
      assert len(run_dbt(["run"])) == 4
      check_relations_equal_with_relations(project.adapter, [
          project.adapter.Relation.create(
            database=ALT_DATABASE,
            schema=project.test_schema,
            identifier=self.check_caps(project, "seed")
        ),
          project.adapter.Relation.create(
            schema=project.test_schema,
            identifier=self.check_caps(project, "view_1")
        ),
          project.adapter.Relation.create(
            database=ALT_DATABASE,
            schema=project.test_schema,
            identifier=self.check_caps(project, "view_2")
        ),
          project.adapter.Relation.create(
            schema=project.test_schema,
            identifier=self.check_caps(project, "view_3")
        ),
          project.adapter.Relation.create(
            database=ALT_DATABASE,
            schema=project.test_schema,
            identifier=self.check_caps(project, "view_4")
        )
      ])

  def test_snwoflake_database_override(self, project, clean_up):
    self.run_database_override(project)


class BaseProjectModelOverrideSnowflake(BaseOverrideDatabaseSnowflake):
  def run_database_override(self, project):
        run_dbt(["seed"])
        result = run_dbt(["run"])
        assert len(result) == 4
        self.assertExpectedRelations(project)

  def assertExpectedRelations(self, project):
        check_relations_equal_with_relations(project.adapter, [
            project.adapter.Relation.create(
                schema=project.test_schema,
                identifier=self.check_caps(project, "seed")
            ),
            project.adapter.Relation.create(
                database=ALT_DATABASE,
                schema=project.test_schema,
                identifier=self.check_caps(project, "view_1")
            ),
            project.adapter.Relation.create(
                database=ALT_DATABASE,
                schema=project.test_schema,
                identifier=self.check_caps(project, "view_2")
            ),
            project.adapter.Relation.create(
                schema=project.test_schema,
                identifier=self.check_caps(project, "view_3")
            ),
            project.adapter.Relation.create(
                database=ALT_DATABASE,
                schema=project.test_schema,
                identifier=self.check_caps(project, "view_4")
            )
        ])


class TestProjectModelOverrideSnowflake(BaseProjectModelOverrideSnowflake):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "config-version": 2,
            "vars": {
                "alternate_db": ALT_DATABASE,
            },
            "models": {
                "database": ALT_DATABASE,
                "test": {
                    "subfolder": {
                        "database": "{{ target.database }}"
                    }
                }
            },
            "seed-paths": ["seeds"],
            "vars": {
                "alternate_db": ALT_DATABASE,
            },
            "quoting": {
                "database": True,
            },
            "seeds": {
                "quote_columns": False,
            }
        }

    def test_snowflake_database_override(self, project, clean_up):
      self.run_database_override(project)


class TestProjectModelAliasOverrideSnowflake(BaseProjectModelOverrideSnowflake):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "config-version": 2,
            "vars": {
                "alternate_db": ALT_DATABASE,
            },
            "models": {
                "project": ALT_DATABASE,
                "test": {
                    "subfolder": {
                        "project": "{{ target.database }}"
                    }
                }
            },
            "seed-paths": ["seeds"],
            "vars": {
                "alternate_db": ALT_DATABASE,
            },
            "quoting": {
                "database": True,
            },
            "seeds": {
                "quote_columns": False,
            }
        }

    def test_snowflake_project_override(self, project, clean_up):
        self.run_database_override(project)
