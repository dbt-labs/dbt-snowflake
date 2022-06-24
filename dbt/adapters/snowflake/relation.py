from dataclasses import dataclass
from dbt.adapters.base.relation import BaseRelation, Policy
from dbt.dataclass_schema import StrEnum
from dbt.utils import classproperty
from dbt.contracts.relation import RelationType


@dataclass
class SnowflakeQuotePolicy(Policy):
    database: bool = False
    schema: bool = False
    identifier: bool = False
    

@dataclass(frozen=True, eq=False, repr=False)
class SnowflakeRelation(BaseRelation):
    quote_policy: SnowflakeQuotePolicy = SnowflakeQuotePolicy()

    @property
    def is_materializedview(self) -> bool:
        return self.type == RelationType.MaterializedView

    @classproperty
    def MaterializedView(cls) -> str:
        return str(RelationType.MaterializedView)