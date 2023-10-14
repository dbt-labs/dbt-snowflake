from dataclasses import dataclass
from typing import Optional

import agate
from dbt.adapters.base.relation import Policy
from dbt.adapters.relation_configs import RelationConfigBase
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
    def include_policy(cls) -> Policy:
        return SnowflakeIncludePolicy()

    @classmethod
    def quote_policy(cls) -> Policy:
        return SnowflakeQuotePolicy()

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
