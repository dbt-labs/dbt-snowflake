from dataclasses import dataclass
from typing import Mapping, Any, Optional, List, Union, Dict, FrozenSet, Tuple

import agate

from dbt.adapters.base.impl import AdapterConfig, ConstraintSupport  # type: ignore
from dbt.adapters.base.meta import available
from dbt.adapters.capability import CapabilityDict, CapabilitySupport, Support, Capability
from dbt.adapters.sql import SQLAdapter  # type: ignore
from dbt.adapters.sql.impl import (
    LIST_SCHEMAS_MACRO_NAME,
    LIST_RELATIONS_MACRO_NAME,
)

from dbt.adapters.snowflake import SnowflakeConnectionManager
from dbt.adapters.snowflake import SnowflakeRelation
from dbt.adapters.snowflake import SnowflakeColumn
from dbt_common.contracts.constraints import ConstraintType
from dbt_common.exceptions import CompilationError, DbtDatabaseError, DbtRuntimeError
from dbt_common.utils import filter_null_values


@dataclass
class SnowflakeConfig(AdapterConfig):
    transient: Optional[bool] = None
    cluster_by: Optional[Union[str, List[str]]] = None
    automatic_clustering: Optional[bool] = None
    secure: Optional[bool] = None
    copy_grants: Optional[bool] = None
    snowflake_warehouse: Optional[str] = None
    query_tag: Optional[str] = None
    tmp_relation_type: Optional[str] = None
    merge_update_columns: Optional[str] = None
    target_lag: Optional[str] = None


class SnowflakeAdapter(SQLAdapter):
    Relation = SnowflakeRelation
    Column = SnowflakeColumn
    ConnectionManager = SnowflakeConnectionManager

    AdapterSpecificConfigs = SnowflakeConfig

    CONSTRAINT_SUPPORT = {
        ConstraintType.check: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.not_null: ConstraintSupport.ENFORCED,
        ConstraintType.unique: ConstraintSupport.NOT_ENFORCED,
        ConstraintType.primary_key: ConstraintSupport.NOT_ENFORCED,
        ConstraintType.foreign_key: ConstraintSupport.NOT_ENFORCED,
    }

    _capabilities: CapabilityDict = CapabilityDict(
        {
            Capability.SchemaMetadataByRelations: CapabilitySupport(support=Support.Full),
            Capability.TableLastModifiedMetadata: CapabilitySupport(support=Support.Full),
            Capability.TableLastModifiedMetadataBatch: CapabilitySupport(support=Support.Full),
        }
    )

    @classmethod
    def date_function(cls):
        return "CURRENT_TIMESTAMP()"

    @classmethod
    def _catalog_filter_table(
        cls, table: agate.Table, used_schemas: FrozenSet[Tuple[str, str]]
    ) -> agate.Table:
        # On snowflake, users can set QUOTED_IDENTIFIERS_IGNORE_CASE, so force
        # the column names to their lowercased forms.
        lowered = table.rename(column_names=[c.lower() for c in table.column_names])
        return super()._catalog_filter_table(lowered, used_schemas)

    def _make_match_kwargs(self, database, schema, identifier):
        quoting = self.config.quoting
        if identifier is not None and quoting["identifier"] is False:
            identifier = identifier.upper()

        if schema is not None and quoting["schema"] is False:
            schema = schema.upper()

        if database is not None and quoting["database"] is False:
            database = database.upper()

        return filter_null_values(
            {"identifier": identifier, "schema": schema, "database": database}
        )

    def _get_warehouse(self) -> str:
        _, table = self.execute("select current_warehouse() as warehouse", fetch=True)
        if len(table) == 0 or len(table[0]) == 0:
            # can this happen?
            raise DbtRuntimeError("Could not get current warehouse: no results")
        return str(table[0][0])

    def _use_warehouse(self, warehouse: str):
        """Use the given warehouse. Quotes are never applied."""
        self.execute("use warehouse {}".format(warehouse))

    def pre_model_hook(self, config: Mapping[str, Any]) -> Optional[str]:
        default_warehouse = self.config.credentials.warehouse
        warehouse = config.get("snowflake_warehouse", default_warehouse)
        if warehouse == default_warehouse or warehouse is None:
            return None
        previous = self._get_warehouse()
        self._use_warehouse(warehouse)
        return previous

    def post_model_hook(self, config: Mapping[str, Any], context: Optional[str]) -> None:
        if context is not None:
            self._use_warehouse(context)

    def list_schemas(self, database: str) -> List[str]:
        try:
            results = self.execute_macro(LIST_SCHEMAS_MACRO_NAME, kwargs={"database": database})
        except DbtDatabaseError as exc:
            msg = f"Database error while listing schemas in database " f'"{database}"\n{exc}'
            raise DbtRuntimeError(msg)
        # this uses 'show terse schemas in database', and the column name we
        # want is 'name'

        return [row["name"] for row in results]

    def get_columns_in_relation(self, relation):
        try:
            return super().get_columns_in_relation(relation)
        except DbtDatabaseError as exc:
            if "does not exist or not authorized" in str(exc):
                return []
            else:
                raise

    def list_relations_without_caching(self, schema_relation: SnowflakeRelation) -> List[SnowflakeRelation]:  # type: ignore
        kwargs = {"schema_relation": schema_relation}
        try:
            results = self.execute_macro(LIST_RELATIONS_MACRO_NAME, kwargs=kwargs)
        except DbtDatabaseError as exc:
            # if the schema doesn't exist, we just want to return.
            # Alternatively, we could query the list of schemas before we start
            # and skip listing the missing ones, which sounds expensive.
            if "Object does not exist" in str(exc):
                return []
            raise

        # this can be reduced to always including `is_dynamic` once bundle `2024_03` is mandatory
        columns = ["database_name", "schema_name", "name", "kind"]
        if "is_dynamic" in results.column_names:  # type: ignore
            columns.append("is_dynamic")

        return [self._parse_list_relations_result(result) for result in results.select(columns)]  # type: ignore

    def _parse_list_relations_result(self, result: agate.Row) -> SnowflakeRelation:
        # this can be reduced to always including `is_dynamic` once bundle `2024_03` is mandatory
        try:
            database, schema, identifier, relation_type, is_dynamic = result
        except ValueError:
            database, schema, identifier, relation_type = result
            is_dynamic = "N"

        try:
            relation_type = self.Relation.get_relation_type(relation_type.lower())
        except ValueError:
            relation_type = self.Relation.External

        if relation_type == self.Relation.Table and is_dynamic == "Y":
            relation_type = self.Relation.DynamicTable

        quote_policy = {"database": True, "schema": True, "identifier": True}
        return self.Relation.create(
            database=database,
            schema=schema,
            identifier=identifier,
            type=relation_type,
            quote_policy=quote_policy,
        )

    def quote_seed_column(self, column: str, quote_config: Optional[bool]) -> str:
        quote_columns: bool = False
        if isinstance(quote_config, bool):
            quote_columns = quote_config
        elif quote_config is None:
            pass
        else:
            msg = (
                f'The seed configuration value of "quote_columns" has an '
                f"invalid type {type(quote_config)}"
            )
            raise CompilationError(msg)

        if quote_columns:
            return self.quote(column)
        else:
            return column

    @available
    def standardize_grants_dict(self, grants_table: agate.Table) -> dict:
        grants_dict: Dict[str, Any] = {}

        for row in grants_table:
            grantee = row["grantee_name"]
            granted_to = row["granted_to"]
            privilege = row["privilege"]
            if privilege != "OWNERSHIP" and granted_to != "SHARE":
                if privilege in grants_dict.keys():
                    grants_dict[privilege].append(grantee)
                else:
                    grants_dict.update({privilege: [grantee]})
        return grants_dict

    def timestamp_add_sql(self, add_to: str, number: int = 1, interval: str = "hour") -> str:
        return f"DATEADD({interval}, {number}, {add_to})"

    def submit_python_job(self, parsed_model: dict, compiled_code: str):
        schema = parsed_model["schema"]
        database = parsed_model["database"]
        identifier = parsed_model["alias"]
        python_version = parsed_model["config"].get("python_version", "3.8")

        packages = parsed_model["config"].get("packages", [])
        imports = parsed_model["config"].get("imports", [])
        external_access_integrations = parsed_model["config"].get(
            "external_access_integrations", []
        )
        secrets = parsed_model["config"].get("secrets", {})
        # adding default packages we need to make python model work
        default_packages = ["snowflake-snowpark-python"]
        package_names = [package.split("==")[0] for package in packages]
        for default_package in default_packages:
            if default_package not in package_names:
                packages.append(default_package)
        packages = "', '".join(packages)
        imports = "', '".join(imports)
        external_access_integrations = ", ".join(external_access_integrations)
        secrets = ", ".join(f"'{key}' = {value}" for key, value in secrets.items())

        # we can't pass empty imports, external_access_integrations or secrets clause to snowflake
        if imports:
            imports = f"IMPORTS = ('{imports}')"
        if external_access_integrations:
            # Black is trying to make this a tuple.
            # fmt: off
            external_access_integrations = f"EXTERNAL_ACCESS_INTEGRATIONS = ({external_access_integrations})"
        if secrets:
            secrets = f"SECRETS = ({secrets})"

        if self.config.args.SEND_ANONYMOUS_USAGE_STATS:
            snowpark_telemetry_string = "dbtLabs_dbtPython"
            snowpark_telemetry_snippet = f"""
import sys
sys._xoptions['snowflake_partner_attribution'].append("{snowpark_telemetry_string}")"""
        else:
            snowpark_telemetry_snippet = ""

        common_procedure_code = f"""
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '{python_version}'
PACKAGES = ('{packages}')
{external_access_integrations}
{secrets}
{imports}
HANDLER = 'main'
EXECUTE AS CALLER
AS
$$
{snowpark_telemetry_snippet}

{compiled_code}
$$"""

        use_anonymous_sproc = parsed_model["config"].get("use_anonymous_sproc", True)
        if use_anonymous_sproc:
            proc_name = f"{identifier}__dbt_sp"
            python_stored_procedure = f"""
WITH {proc_name} AS PROCEDURE ()
{common_procedure_code}
CALL {proc_name}();
            """
        else:
            proc_name = f"{database}.{schema}.{identifier}__dbt_sp"
            python_stored_procedure = f"""
CREATE OR REPLACE PROCEDURE {proc_name} ()
{common_procedure_code};
CALL {proc_name}();

            """
        response, _ = self.execute(python_stored_procedure, auto_begin=False, fetch=False)
        if not use_anonymous_sproc:
            self.execute(
                f"drop procedure if exists {proc_name}()",
                auto_begin=False,
                fetch=False,
            )
        return response

    def valid_incremental_strategies(self):
        return ["append", "merge", "delete+insert"]

    def debug_query(self):
        """Override for DebugTask method"""
        self.execute("select 1 as id")
