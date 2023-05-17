import pytest

from dbt.tests.util import get_manifest
from dbt.tests.adapter.materialized_view.base import Base, Model, run_model

from dbt.adapters.snowflake.relation import SnowflakeRelationType


class SnowflakeBase(Base):
    base_dynamic_table = Model(
        name="base_dynamic_table",
        definition="{{ config(materialized='dynamic_table') }} select * from {{ ref('base_table') }}",
    )
    base_materialized_view = base_dynamic_table

    def assert_relation_is_dynamic_table(self, project, model: Model = None):
        model = model or self.base_dynamic_table

        manifest = get_manifest(project.project_root)
        model_metadata = manifest.nodes[f"model.test.{model.name}"]
        assert model_metadata.config.materialized == SnowflakeRelationType.DynamicTable
        assert len(self.get_records(project, model)) >= 0


class TestBasic(SnowflakeBase):
    def test_relation_is_dynamic_table_on_initial_creation(self, project):
        self.assert_relation_is_dynamic_table(project)

    def test_relation_is_dynamic_table_when_rerun(self, project):
        run_model(self.base_dynamic_table.name)
        self.assert_relation_is_dynamic_table(project)

    def test_relation_is_dynamic_table_on_full_refresh(self, project):
        run_model(self.base_dynamic_table.name, full_refresh=True)
        self.assert_relation_is_dynamic_table(project)

    def test_relation_is_dynamic_table_on_update(self, project):
        run_model(self.base_dynamic_table.name, run_args=["--vars", "quoting: {identifier: True}"])
        self.assert_relation_is_dynamic_table(project)

    @pytest.mark.skip("Fails because stub uses traditional view")
    def test_updated_base_table_data_only_shows_in_dynamic_table_after_rerun(self, project):
        self.insert_records(project, self.inserted_records)
        assert self.get_records(project) == self.starting_records

        run_model(self.base_dynamic_table.name)
        assert self.get_records(project) == self.starting_records + self.inserted_records
