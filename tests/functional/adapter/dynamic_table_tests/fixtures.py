import pytest

from dbt.tests.util import get_manifest, relation_from_name
from dbt.tests.adapter.materialized_view.base import Base, Model
from dbt.tests.adapter.materialized_view.on_configuration_change import OnConfigurationChangeBase

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


class SnowflakeOnConfigurationChangeBase(SnowflakeBase, OnConfigurationChangeBase):
    @pytest.fixture(scope="function")
    def configuration_changes(self, project):
        pass

    @pytest.fixture(scope="function")
    def configuration_change_message(self, project):
        # We need to do this because the default quote policy is overriden
        # in `SnowflakeAdapter.list_relations_without_caching`; we wind up with
        # an uppercase quoted name when supplied with a lowercase name with un-quoted quote policy.
        relation = relation_from_name(project.adapter, self.base_materialized_view.name)
        database, schema, name = str(relation).split(".")
        relation_upper = f'"{database.upper()}"."{schema.upper()}"."{name.upper()}"'
        return f"Determining configuration changes on: {relation_upper}"
