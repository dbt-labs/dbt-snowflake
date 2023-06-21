from dataclasses import dataclass
from typing import Optional

from dbt.adapters.relation_configs import RelationConfigBase, RelationResults
from dbt.contracts.graph.nodes import ModelNode
from dbt.contracts.relation import ComponentName

from dbt.adapters.snowflake.relation_configs.policies import (
    SnowflakeIncludePolicy,
    SnowflakeQuotePolicy,
)


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SnowflakeRelationConfigBase(RelationConfigBase):
    """
    This base class implements a few boilerplate methods and provides some light structure for Snowflake relations.
    """

    @classmethod
    def from_model_node(cls, model_node: ModelNode) -> "RelationConfigBase":
        relation_config = cls.parse_model_node(model_node)
        relation = cls.from_dict(relation_config)
        return relation

    @classmethod
    def parse_model_node(cls, model_node: ModelNode) -> dict:
        raise NotImplementedError(
            "`parse_model_node()` needs to be implemented on this RelationConfigBase instance"
        )

    @classmethod
    def from_relation_results(cls, relation_results: RelationResults) -> "RelationConfigBase":
        relation_config = cls.parse_relation_results(relation_results)
        relation = cls.from_dict(relation_config)
        return relation

    @classmethod
    def parse_relation_results(cls, relation_results: RelationResults) -> dict:
        raise NotImplementedError(
            "`parse_relation_results()` needs to be implemented on this RelationConfigBase instance"
        )

    @staticmethod
    def _render_part(component: ComponentName, value: str) -> Optional[str]:
        if SnowflakeIncludePolicy().get_part(component):
            if SnowflakeQuotePolicy().get_part(component):
                return f'"{value}"'
            return value.upper()
        return None
