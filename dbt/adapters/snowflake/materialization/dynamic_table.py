from abc import ABC
from dataclasses import dataclass
from typing import Any, Dict, Optional

from dbt.adapters.materialization.models import MaterializedViewMaterialization
from dbt.adapters.relation.factory import RelationFactory
from dbt.adapters.relation.models import RelationRef
from dbt.contracts.graph.nodes import ParsedNode
from dbt.dataclass_schema import StrEnum


class SnowflakeMaterializationType(StrEnum):
    """
    This overlaps with `RelationType` for several values (e.g. `View`); however, they are not the same.
    For example, a materialization type of `Incremental` would be associated with a relation type of `Table`.
    """

    View = "view"
    Table = "table"
    Incremental = "incremental"
    Seed = "seed"
    DynamicTable = "dynamic_table"


@dataclass
class DynamicTableMaterialization(MaterializedViewMaterialization, ABC):
    """
    This config identifies the minimal materialization parameters required for dbt-snowflake to function as well
    as built-ins that make macros more extensible. Additional parameters may be added by subclassing for your adapter.

    This is only overridden because we have a different set of `MaterializationTypes`
    """

    @classmethod
    def parse_node(
        cls,
        node: ParsedNode,
        relation_factory: RelationFactory,
        existing_relation_ref: Optional[RelationRef] = None,
    ) -> Dict[str, Any]:
        config_dict = super().parse_node(node, relation_factory, existing_relation_ref)
        config_dict.update({"type": SnowflakeMaterializationType.DynamicTable})
        return config_dict
