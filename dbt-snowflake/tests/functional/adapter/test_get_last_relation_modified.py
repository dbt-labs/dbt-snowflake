import os
import pytest
from unittest import mock

from dbt.adapters.snowflake.impl import SnowflakeAdapter
from dbt.adapters.capability import Capability, CapabilityDict
from dbt.cli.main import dbtRunner


freshness_via_metadata_schema_yml = """
sources:
  - name: test_source
    freshness:
      warn_after: {count: 10, period: hour}
      error_after: {count: 1, period: day}
    schema: "{{ env_var('DBT_GET_LAST_RELATION_TEST_SCHEMA') }}"
    tables:
      - name: test_table
"""

freshness_metadata_schema_batch_yml = """
sources:
  - name: test_source
    freshness:
      warn_after: {count: 10, period: hour}
      error_after: {count: 1, period: day}
    schema: "{{ env_var('DBT_GET_LAST_RELATION_TEST_SCHEMA') }}"
    tables:
      - name: test_table
      - name: test_table2
      - name: test_table_with_loaded_at_field
        loaded_at_field: my_loaded_at_field
"""


class SetupGetLastRelationModified:
    @pytest.fixture(scope="class", autouse=True)
    def set_env_vars(self, project):
        os.environ["DBT_GET_LAST_RELATION_TEST_SCHEMA"] = project.test_schema
        yield
        del os.environ["DBT_GET_LAST_RELATION_TEST_SCHEMA"]

    @pytest.fixture(scope="class")
    def custom_schema(self, project, set_env_vars):
        with project.adapter.connection_named("__test"):
            relation = project.adapter.Relation.create(
                database=project.database, schema=os.environ["DBT_GET_LAST_RELATION_TEST_SCHEMA"]
            )
            project.adapter.drop_schema(relation)
            project.adapter.create_schema(relation)

        yield relation.schema

        with project.adapter.connection_named("__test"):
            project.adapter.drop_schema(relation)


class TestGetLastRelationModified(SetupGetLastRelationModified):
    @pytest.fixture(scope="class")
    def models(self):
        return {"schema.yml": freshness_via_metadata_schema_yml}

    def test_get_last_relation_modified(self, project, set_env_vars, custom_schema):
        project.run_sql(
            f"create table {custom_schema}.test_table (id integer autoincrement, name varchar(100) not null);"
        )

        warning_or_error = False

        def probe(e):
            nonlocal warning_or_error
            if e.info.level in ["warning", "error"]:
                warning_or_error = True

        runner = dbtRunner(callbacks=[probe])
        runner.invoke(["source", "freshness"])

        # The 'source freshness' command should succeed without warnings or errors.
        assert not warning_or_error


class TestGetLastRelationModifiedBatch(SetupGetLastRelationModified):
    @pytest.fixture(scope="class")
    def models(self):
        return {"schema.yml": freshness_metadata_schema_batch_yml}

    def get_freshness_result_for_table(self, table_name, results):
        for result in results:
            if result.node.name == table_name:
                return result
        return None

    def test_get_last_relation_modified_batch(self, project, set_env_vars, custom_schema):
        project.run_sql(
            f"create table {custom_schema}.test_table (id integer autoincrement, name varchar(100) not null);"
        )
        project.run_sql(
            f"create table {custom_schema}.test_table2 (id integer autoincrement, name varchar(100) not null);"
        )
        project.run_sql(
            f"create table {custom_schema}.test_table_with_loaded_at_field as (select 1 as id, timestamp '2009-09-15 10:59:43' as my_loaded_at_field);"
        )

        runner = dbtRunner()
        freshness_results_batch = runner.invoke(["source", "freshness"]).result

        assert len(freshness_results_batch) == 3
        test_table_batch_result = self.get_freshness_result_for_table(
            "test_table", freshness_results_batch
        )
        test_table2_batch_result = self.get_freshness_result_for_table(
            "test_table2", freshness_results_batch
        )
        test_table_with_loaded_at_field_batch_result = self.get_freshness_result_for_table(
            "test_table_with_loaded_at_field", freshness_results_batch
        )

        # Remove TableLastModifiedMetadataBatch and run freshness on same input without batch strategy
        capabilities_no_batch = CapabilityDict(
            {
                capability: support
                for capability, support in SnowflakeAdapter.capabilities().items()
                if capability != Capability.TableLastModifiedMetadataBatch
            }
        )
        with mock.patch.object(
            SnowflakeAdapter, "capabilities", return_value=capabilities_no_batch
        ):
            freshness_results = runner.invoke(["source", "freshness"]).result

        assert len(freshness_results) == 3
        test_table_result = self.get_freshness_result_for_table("test_table", freshness_results)
        test_table2_result = self.get_freshness_result_for_table("test_table2", freshness_results)
        test_table_with_loaded_at_field_result = self.get_freshness_result_for_table(
            "test_table_with_loaded_at_field", freshness_results
        )

        # assert results between batch vs non-batch freshness strategy are equivalent
        assert test_table_result.status == test_table_batch_result.status
        assert test_table_result.max_loaded_at == test_table_batch_result.max_loaded_at

        assert test_table2_result.status == test_table2_batch_result.status
        assert test_table2_result.max_loaded_at == test_table2_batch_result.max_loaded_at

        assert (
            test_table_with_loaded_at_field_batch_result.status
            == test_table_with_loaded_at_field_result.status
        )
        assert (
            test_table_with_loaded_at_field_batch_result.max_loaded_at
            == test_table_with_loaded_at_field_result.max_loaded_at
        )
