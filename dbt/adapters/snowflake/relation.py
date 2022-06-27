from typing import Optional

from dataclasses import dataclass
from dbt.adapters.base.relation import BaseRelation, Policy
from dbt.dataclass_schema import StrEnum
from dbt.utils import classproperty


class SnowflakeRelationType(StrEnum):
    Table = "table"
    View = "view"
    CTE = "cte"
    MaterializedView = "materialized view"
    External = "external"


@dataclass
class SnowflakeQuotePolicy(Policy):
    database: bool = False
    schema: bool = False
    identifier: bool = False


@dataclass(frozen=True, eq=False, repr=False)
class SnowflakeRelation(BaseRelation):
    quote_policy: SnowflakeQuotePolicy = SnowflakeQuotePolicy()
    type: Optional[SnowflakeRelationType] = None

    def is_materializedview(self) -> bool:
        return self.type == SnowflakeRelationType.MaterializedView

    @classproperty
    def MaterializedView(cls) -> str:
        return str(SnowflakeRelationType.MaterializedView)
