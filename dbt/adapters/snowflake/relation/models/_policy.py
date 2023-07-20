from dataclasses import dataclass

from dbt.adapters.base.relation import Policy


@dataclass
class SnowflakeQuotePolicy(Policy):
    database: bool = False
    schema: bool = False
    identifier: bool = False
