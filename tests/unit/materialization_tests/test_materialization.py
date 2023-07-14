from dataclasses import replace

from dbt.adapters.materialization.models import MaterializationBuildStrategy

from dbt.adapters.snowflake.materialization import DynamicTableMaterialization


def test_dynamic_table_create(dynamic_table_compiled_node, relation_factory):
    materialization = DynamicTableMaterialization.from_node(
        dynamic_table_compiled_node, relation_factory
    )
    assert materialization.build_strategy == MaterializationBuildStrategy.Create
    assert materialization.should_revoke_grants is False


def test_dynamic_table_replace(dynamic_table_compiled_node, relation_factory, view_ref):
    materialization = DynamicTableMaterialization.from_node(
        dynamic_table_compiled_node, relation_factory, view_ref
    )
    assert materialization.build_strategy == MaterializationBuildStrategy.Replace
    assert materialization.should_revoke_grants is True


def test_dynamic_table_alter(
    dynamic_table_compiled_node, relation_factory, dynamic_table_relation
):
    altered_dynamic_table = replace(dynamic_table_relation, warehouse="peewees_playhouse")

    materialization = DynamicTableMaterialization.from_node(
        dynamic_table_compiled_node, relation_factory, altered_dynamic_table
    )
    assert materialization.build_strategy == MaterializationBuildStrategy.Alter
    assert materialization.should_revoke_grants is True
