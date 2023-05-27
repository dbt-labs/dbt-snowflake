from dataclasses import dataclass, field
from typing import Optional, List, Dict, Union

import agate

from dbt.contracts.graph.model_config import NodeConfig
from dbt.adapters.base.relation import BaseRelation, Policy
from dbt.dataclass_schema import StrEnum
from dbt.utils import classproperty

from dbt.adapters.snowflake.model_config import DynamicTableRefreshStrategy


class SnowflakeRelationType(StrEnum):
    Table = "table"
    View = "view"
    CTE = "cte"
    External = "external"
    DynamicTable = "dynamic_table"


@dataclass
class SnowflakeQuotePolicy(Policy):
    database: bool = False
    schema: bool = False
    identifier: bool = False


@dataclass(frozen=True, eq=False, repr=False)
class SnowflakeRelation(BaseRelation):
    RefreshStrategyUpdates = Dict[str, Union[str, DynamicTableRefreshStrategy]]

    type: Optional[SnowflakeRelationType] = None  # type: ignore
    quote_policy: SnowflakeQuotePolicy = field(default_factory=lambda: SnowflakeQuotePolicy())

    @property
    def is_dynamic_table(self) -> bool:
        return self.type == SnowflakeRelationType.DynamicTable

    @classproperty
    def DynamicTable(cls) -> str:
        return str(SnowflakeRelationType.DynamicTable)

    def get_refresh_strategy_updates(
        self, last_refresh: agate.Table, new_config: NodeConfig
    ) -> List[Optional[RefreshStrategyUpdates]]:
        last_refresh_metadata = last_refresh[0]
        existing_refresh_strategy = last_refresh_metadata.state
        new_refresh_strategy = new_config.get("refresh_strategy")
        if existing_refresh_strategy == new_refresh_strategy:
            return []
        return [
            {"action": "replace", "context": DynamicTableRefreshStrategy(new_refresh_strategy)}
        ]
