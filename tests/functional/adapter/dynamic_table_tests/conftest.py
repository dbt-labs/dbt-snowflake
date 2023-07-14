import pytest

from dbt.adapters.materialization import MaterializationFactory
from dbt.adapters.relation.models import RelationRef
from dbt.adapters.relation.factory import RelationFactory

from dbt.adapters.snowflake.materialization import (
    DynamicTableMaterialization,
    SnowflakeMaterializationType,
)
from dbt.adapters.snowflake.relation import models


@pytest.fixture(scope="class")
def relation_factory():
    return RelationFactory(
        relation_types=models.SnowflakeRelationType,
        relation_models={
            models.SnowflakeRelationType.DynamicTable: models.SnowflakeDynamicTableRelation,
        },
        relation_changesets={
            models.SnowflakeRelationType.DynamicTable: models.SnowflakeDynamicTableRelationChangeset,
        },
        relation_can_be_renamed={
            models.SnowflakeRelationType.Table,
            models.SnowflakeRelationType.View,
        },
        render_policy=models.SnowflakeRenderPolicy,
    )


@pytest.fixture
def materialization_factory(relation_factory):
    return MaterializationFactory(
        relation_factory=relation_factory,
        materialization_types=SnowflakeMaterializationType,
        materialization_models={
            SnowflakeMaterializationType.DynamicTable: DynamicTableMaterialization
        },
    )


@pytest.fixture(scope="class")
def my_dynamic_table(project, relation_factory) -> RelationRef:
    relation_ref = relation_factory.make_ref(
        name="my_dynamic_table",
        schema_name=project.test_schema,
        database_name=project.database,
        relation_type=models.SnowflakeRelationType.DynamicTable,
    )
    return relation_ref


@pytest.fixture(scope="class")
def my_view(project, relation_factory) -> RelationRef:
    return relation_factory.make_ref(
        name="my_view",
        schema_name=project.test_schema,
        database_name=project.database,
        relation_type=models.SnowflakeRelationType.View,
    )


@pytest.fixture(scope="class")
def my_table(project, relation_factory) -> RelationRef:
    return relation_factory.make_ref(
        name="my_table",
        schema_name=project.test_schema,
        database_name=project.database,
        relation_type=models.SnowflakeRelationType.Table,
    )


@pytest.fixture(scope="class")
def my_seed(project, relation_factory) -> RelationRef:
    return relation_factory.make_ref(
        name="my_seed",
        schema_name=project.test_schema,
        database_name=project.database,
        relation_type=models.SnowflakeRelationType.Table,
    )
