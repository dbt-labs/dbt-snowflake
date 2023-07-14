import agate
import pytest

from dbt.adapters.materialization.factory import MaterializationFactory
from dbt.adapters.relation.factory import RelationFactory
from dbt.contracts.files import FileHash
from dbt.contracts.graph.nodes import DependsOn, CompiledNode, NodeConfig
from dbt.node_types import NodeType

from dbt.adapters.snowflake.materialization import (
    DynamicTableMaterialization,
    SnowflakeMaterializationType,
)
from dbt.adapters.snowflake.relation.models import (
    SnowflakeDynamicTableRelation,
    SnowflakeDynamicTableRelationChangeset,
    SnowflakeRelationType,
    SnowflakeRenderPolicy,
)


@pytest.fixture
def relation_factory():
    return RelationFactory(
        relation_types=SnowflakeRelationType,
        relation_models={
            SnowflakeRelationType.DynamicTable: SnowflakeDynamicTableRelation,
        },
        relation_changesets={
            SnowflakeRelationType.DynamicTable: SnowflakeDynamicTableRelationChangeset,
        },
        relation_can_be_renamed={
            SnowflakeRelationType.Table,
            SnowflakeRelationType.View,
        },
        render_policy=SnowflakeRenderPolicy,
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


@pytest.fixture
def dynamic_table_ref(relation_factory):
    return relation_factory.make_ref(
        name="my_dynamic_table",
        schema_name="my_schema",
        database_name="my_database",
        relation_type=SnowflakeRelationType.DynamicTable,
    )


@pytest.fixture
def view_ref(relation_factory):
    return relation_factory.make_ref(
        name="my_view",
        schema_name="my_schema",
        database_name="my_database",
        relation_type=SnowflakeRelationType.View,
    )


@pytest.fixture
def dynamic_table_compiled_node():
    return CompiledNode(
        alias="my_dynamic_table",
        name="my_dynamic_table",
        database="my_database",
        schema="my_schema",
        resource_type=NodeType.Model,
        unique_id="model.root.my_dynamic_table",
        fqn=["root", "my_dynamic_table"],
        package_name="root",
        original_file_path="my_dynamic_table.sql",
        refs=[],
        sources=[],
        depends_on=DependsOn(),
        config=NodeConfig.from_dict(
            {
                "enabled": True,
                "materialized": "dynamic_table",
                "persist_docs": {},
                "post-hook": [],
                "pre-hook": [],
                "vars": {},
                "quoting": {},
                "column_types": {},
                "tags": [],
                "target_lag": "4 minutes",
                "snowflake_warehouse": "my_warehouse",
                "on_configuration_change": "continue",
            }
        ),
        tags=[],
        path="my_dynamic_table.sql",
        language="sql",
        raw_code="select 42 from meaning_of_life",
        compiled_code="select 42 from meaning_of_life",
        description="",
        columns={},
        checksum=FileHash.from_contents(""),
    )


@pytest.fixture
def dynamic_table_describe_relation_results():
    sql = """
    create dynamic table my_schema.my_table
        target_lag = '1 minute'
        warehouse = my_warehouse
    as (
        select 4 as id, 2 as other_id from meaning_of_life
    );
    """
    dynamic_table_agate = agate.Table.from_object(
        [
            {
                "name": "my_dynamic_table",
                "schema_name": "my_schema",
                "database_name": "my_database",
                "query": sql,
                "target_lag": "2 minutes",
                "warehouse": "my_warehouse",
            }
        ],
        column_types={"target_lag": agate.Text()},
    )
    return {"relation": dynamic_table_agate}


@pytest.fixture
def dynamic_table_relation(relation_factory, dynamic_table_describe_relation_results):
    return relation_factory.make_from_describe_relation_results(
        dynamic_table_describe_relation_results, SnowflakeRelationType.DynamicTable
    )


"""
Make sure the fixtures at least work, more thorough testing is done elsewhere
"""


def test_relation_factory(relation_factory):
    assert (
        relation_factory._get_relation_model(SnowflakeRelationType.DynamicTable)
        == SnowflakeDynamicTableRelation
    )


def test_materialization_factory(materialization_factory):
    relation_model = materialization_factory.relation_factory._get_relation_model(
        SnowflakeRelationType.DynamicTable
    )
    assert relation_model == SnowflakeDynamicTableRelation


def test_dynamic_table_ref(dynamic_table_ref):
    assert dynamic_table_ref.name == "my_dynamic_table"


def test_dynamic_table_compiled_node(dynamic_table_compiled_node):
    assert dynamic_table_compiled_node.name == "my_dynamic_table"


def test_dynamic_table_relation(dynamic_table_relation):
    assert dynamic_table_relation.type == SnowflakeRelationType.DynamicTable
    assert dynamic_table_relation.name == "my_dynamic_table"
