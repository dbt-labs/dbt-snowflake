from dataclasses import dataclass, field
from typing import Optional

from dbt.adapters.base.relation import BaseRelation, Policy
from dbt.dataclass_schema import StrEnum
from dbt.utils import classproperty


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
    type: Optional[SnowflakeRelationType] = None  # type: ignore
    quote_policy: SnowflakeQuotePolicy = field(default_factory=lambda: SnowflakeQuotePolicy())

    @property
    def is_dynamic_table(self) -> bool:
        return self.type == SnowflakeRelationType.DynamicTable

    @classproperty
    def DynamicTable(cls) -> str:
        return str(SnowflakeRelationType.DynamicTable)
