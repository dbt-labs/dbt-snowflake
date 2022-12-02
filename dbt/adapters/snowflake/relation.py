from dataclasses import dataclass
from dbt.adapters.base.relation import BaseRelation, Policy
from dataclasses import field


@dataclass
class SnowflakeQuotePolicy(Policy):
    database: bool = False
    schema: bool = False
    identifier: bool = False


@dataclass(frozen=True, eq=False, repr=False)
class SnowflakeRelation(BaseRelation):
    quote_policy: SnowflakeQuotePolicy = field(default_factory=SnowflakeQuotePolicy)
