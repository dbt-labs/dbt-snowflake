from dataclasses import dataclass
from typing import Any, Dict, Optional
import agate
from dbt.adapters.base.relation import Policy
from dbt.adapters.relation_configs import (
    RelationConfigBase,
    RelationResults,
)

from dbt.adapters.contracts.relation import ComponentName, RelationConfig

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
    def include_policy(cls) -> Policy:
        return SnowflakeIncludePolicy()

    @classmethod
    def quote_policy(cls) -> Policy:
        return SnowflakeQuotePolicy()

    @classmethod
    def from_relation_config(cls, relation_config: RelationConfig):
        relation_config_dict = cls.parse_relation_config(relation_config)
        relation = cls.from_dict(relation_config_dict)
        return relation

    @classmethod
    def parse_relation_config(cls, relation_config: RelationConfig) -> Dict:
        raise NotImplementedError(
            "`parse_relation_config()` needs to be implemented on this RelationConfigBase instance"
        )

    @classmethod
    def from_relation_results(cls, relation_results: RelationResults):
        relation_config = cls.parse_relation_results(relation_results)
        relation = cls.from_dict(relation_config)
        return relation  # type: ignore

    @classmethod
    def parse_relation_results(cls, relation_results: RelationResults) -> Dict[str, Any]:
        raise NotImplementedError(
            "`parse_relation_results()` needs to be implemented on this RelationConfigBase instance"
        )

    @classmethod
    def _render_part(cls, component: ComponentName, value: Optional[str]) -> Optional[str]:
        if cls.include_policy().get_part(component) and value:
            if cls.quote_policy().get_part(component):
                return f'"{value}"'
            return value.lower()
        return None

    @classmethod
    def _get_first_row(cls, results: agate.Table) -> agate.Row:
        try:
            return results.rows[0]
        except IndexError:
            return agate.Row(values=set())
