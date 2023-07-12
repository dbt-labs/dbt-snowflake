from copy import deepcopy
from dataclasses import dataclass
from typing import Dict, Optional, Set, Union

import agate
from dbt.adapters.relation.models import (
    Relation,
    RelationChange,
    RelationChangeAction,
    RelationChangeset,
)
from dbt.adapters.validation import ValidationMixin, ValidationRule
from dbt.contracts.graph.nodes import ModelNode
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.snowflake.relation import SnowflakeRelationType
from dbt.adapters.snowflake.relation.models.policy import SnowflakeRenderPolicy
from dbt.adapters.snowflake.relation.models.schema import SnowflakeSchemaRelation
from dbt.adapters.snowflake.relation.models.target_lag import (
    SnowflakeDynamicTableTargetLagRelation,
    SnowflakeDynamicTableTargetLagRelationChange,
)


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SnowflakeDynamicTableRelation(Relation, ValidationMixin):
    """
    This config follow the specs found here:
    https://docs.snowflake.com/en/sql-reference/sql/create-dynamic-table

    The following parameters are configurable by dbt:
    - name: name of the dynamic table
    - query: the query behind the table
    - lag: the maximum amount of time that the dynamic table’s content should lag behind updates to the base tables
    - warehouse: the name of the warehouse that provides the compute resources for refreshing the dynamic table

    There are currently no non-configurable parameters.
    """

    # attribution
    name: str
    schema: SnowflakeSchemaRelation
    query: str
    target_lag: SnowflakeDynamicTableTargetLagRelation
    warehouse: str

    # configuration
    type = SnowflakeRelationType.DynamicTable  # type: ignore
    can_be_renamed = False
    SchemaParser = SnowflakeSchemaRelation  # type: ignore
    render = SnowflakeRenderPolicy

    @property
    def validation_rules(self) -> Set[ValidationRule]:
        return {
            ValidationRule(
                validation_check=len(self.name or "") > 0,
                validation_error=DbtRuntimeError(
                    f"dbt-snowflake requires a name for a dynamic table, received: {self.name}"
                ),
            ),
            ValidationRule(
                validation_check=all({self.database_name, self.schema_name, self.name}),
                validation_error=DbtRuntimeError(
                    f"dbt-snowflake requires all three parts of an object's path, received:/n"
                    f"    database: {self.database_name}/n"
                    f"    schema: {self.schema_name}/n"
                    f"    identifier: {self.name}/n"
                ),
            ),
        }

    @classmethod
    def from_dict(cls, config_dict: dict) -> "SnowflakeDynamicTableRelation":
        kwargs_dict = deepcopy(config_dict)
        kwargs_dict.update(
            {
                "target_lag": SnowflakeDynamicTableTargetLagRelation.from_dict(
                    config_dict["target_lag"]
                ),
            }
        )

        dynamic_table = super().from_dict(kwargs_dict)
        assert isinstance(dynamic_table, SnowflakeDynamicTableRelation)
        return dynamic_table

    @classmethod
    def parse_model_node(cls, model_node: ModelNode) -> dict:
        config_dict = super().parse_model_node(model_node)

        config_dict.update(
            {
                "warehouse": model_node.config.extra["warehouse"],
            }
        )

        if model_node.config.extra["target_lag"]:
            config_dict.update(
                {"target_lag": SnowflakeDynamicTableTargetLagRelation.parse_model_node(model_node)}
            )

        return config_dict

    @classmethod
    def parse_describe_relation_results(
        cls, describe_relation_results: Dict[str, agate.Table]
    ) -> dict:
        config_dict = super().parse_describe_relation_results(describe_relation_results)
        config_dict.update({"query": cls._parse_query(config_dict["query"])})

        dynamic_table: agate.Row = describe_relation_results["relation"].rows[0]

        config_dict.update(
            {
                "warehouse": dynamic_table.get("warehouse"),
            }
        )

        if dynamic_table.get("target_lag"):
            config_dict.update(
                {
                    "target_lag": SnowflakeDynamicTableTargetLagRelation.parse_describe_relation_results(
                        dynamic_table
                    )
                }
            )

        return config_dict

    @classmethod
    def _parse_query(cls, query: str) -> str:
        """
        Get the select statement from the dynamic table definition in Snowflake.

        Args:
            query: the `create dynamic table` statement from `show dynamic tables`, for example:

            create dynamic table my_dynamic_table
                target_lag = '1 minute'
                warehouse = MY_WAREHOUSE
                as (
                    select * from my_base_table
                )
            ;

        Returns: the `select ...` statement, for example:

            select * from my_base_table

        """
        open_paren = query.find("as (") + len("as (")
        close_paren = query.rindex(")")
        return query[open_paren:close_paren].strip()


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SnowflakeDynamicTableWarehouseRelationChange(RelationChange, ValidationMixin):
    context: str

    @property
    def requires_full_refresh(self) -> bool:
        return False

    @property
    def validation_rules(self) -> Set[ValidationRule]:
        return {
            ValidationRule(
                validation_check=self.action == RelationChangeAction.alter,
                validation_error=DbtRuntimeError(
                    f"Warehouse should only be altered but {self.action} was received."
                ),
            ),
        }


@dataclass
class SnowflakeDynamicTableRelationChangeset(RelationChangeset):
    target_lag: Optional[SnowflakeDynamicTableTargetLagRelationChange] = None
    warehouse: Optional[SnowflakeDynamicTableWarehouseRelationChange] = None
    _requires_full_refresh_override: bool = False

    @classmethod
    def parse_relations(cls, existing_relation: Relation, target_relation: Relation) -> dict:
        try:
            assert isinstance(existing_relation, SnowflakeDynamicTableRelation)
            assert isinstance(target_relation, SnowflakeDynamicTableRelation)
        except AssertionError:
            raise DbtRuntimeError(
                f"Two dynamic table relations were expected, but received:\n"
                f"    existing: {existing_relation}\n"
                f"    new: {target_relation}\n"
            )

        config_dict: Dict[
            str,
            Union[
                SnowflakeDynamicTableTargetLagRelationChange,
                SnowflakeDynamicTableWarehouseRelationChange,
            ],
        ] = {}

        if target_relation.target_lag != existing_relation.target_lag:
            config_dict.update(
                {
                    "target_lag": SnowflakeDynamicTableTargetLagRelationChange(
                        action=RelationChangeAction.alter,
                        context=target_relation.target_lag,
                    )
                }
            )

        if target_relation.warehouse != existing_relation.warehouse:
            config_dict.update(
                {
                    "warehouse": SnowflakeDynamicTableWarehouseRelationChange(
                        action=RelationChangeAction.alter,
                        context=target_relation.warehouse,
                    )
                }
            )

        return config_dict

    @property
    def requires_full_refresh(self) -> bool:
        return any(
            {
                self.target_lag.requires_full_refresh if self.target_lag else False,
                self.warehouse.requires_full_refresh if self.warehouse else False,
                self._requires_full_refresh_override,
            }
        )

    @property
    def is_empty(self) -> bool:
        return not any({self.target_lag, self.warehouse}) and super().is_empty
