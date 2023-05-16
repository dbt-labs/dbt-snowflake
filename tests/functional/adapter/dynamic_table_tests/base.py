import pytest

from dbt.tests.util import get_manifest
from dbt.tests.adapter.materialized_views.base import Base, Model
from dbt.tests.adapter.materialized_views.test_on_configuration_change import (
    OnConfigurationChangeBase,
)

from dbt.adapters.snowflake.relation import SnowflakeRelationType


class SnowflakeBase(Base):
    base_dynamic_table = Model(
        name="base_dynamic_table",
        definition="{{ config(materialized='dynamic_table') }} select * from {{ ref('base_table') }}",
    )
    base_materialized_view = base_dynamic_table

    def assert_relation_is_materialized_view(self, project, model: Model):
        self.assert_relation_is_dynamic_table(project, model)

    def assert_relation_is_dynamic_table(self, project, model: Model):
        manifest = get_manifest(project.project_root)
        model = manifest.nodes[f"model.test.{model.name}"]
        assert model.config.materialized == SnowflakeRelationType.DynamicTable
        assert len(self.get_records(project, model)) >= 0


class SnowflakeOnConfigurationChangeBase(SnowflakeBase, OnConfigurationChangeBase):
    def configuration_changes_apply(self, project):
        pass

    @pytest.fixture(scope="function")
    def configuration_changes_full_refresh(self, project):
        pass
