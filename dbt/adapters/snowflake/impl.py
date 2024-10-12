from dataclasses import dataclass
from typing import Mapping, Any, Optional, List, Union, Dict, FrozenSet, Tuple, TYPE_CHECKING


from dbt.adapters.base.impl import AdapterConfig, ConstraintSupport
from dbt.adapters.base.meta import available
from dbt.adapters.capability import CapabilityDict, CapabilitySupport, Support, Capability
from dbt.adapters.sql import SQLAdapter
from dbt.adapters.sql.impl import (
    LIST_SCHEMAS_MACRO_NAME,
    LIST_RELATIONS_MACRO_NAME,
)
from dbt_common.behavior_flags import BehaviorFlag
from dbt_common.contracts.constraints import ConstraintType
from dbt_common.contracts.metadata import (
    TableMetadata,
    StatsDict,
    StatsItem,
    CatalogTable,
    ColumnMetadata,
)
from dbt_common.exceptions import CompilationError, DbtDatabaseError, DbtRuntimeError
from dbt_common.utils import filter_null_values

from dbt.adapters.snowflake.relation_configs import (
    SnowflakeRelationType,
    TableFormat,
)
from dbt.adapters.snowflake import SnowflakeColumn
from dbt.adapters.snowflake import SnowflakeConnectionManager
from dbt.adapters.snowflake import SnowflakeRelation

if TYPE_CHECKING:
    import agate

SHOW_OBJECT_METADATA_MACRO_NAME = "snowflake__show_object_metadata"


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

    # extended formats
    table_format: Optional[str] = None
    external_volume: Optional[str] = None
    base_location_subpath: Optional[str] = None


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
            Capability.GetCatalogForSingleRelation: CapabilitySupport(support=Support.Full),
        }
    )

    @property
    def _behavior_flags(self) -> List[BehaviorFlag]:
        return [
            {
                "name": "enable_iceberg_materializations",
                "default": False,
                "description": (
                    "Enabling Iceberg materializations introduces latency to metadata queries, "
                    "specifically within the list_relations_without_caching macro. Since Iceberg "
                    "benefits only those actively using it, we've made this behavior opt-in to "
                    "prevent unnecessary latency for other users."
                ),
            }
        ]

    @classmethod
    def date_function(cls):
        return "CURRENT_TIMESTAMP()"

    @classmethod
    def _catalog_filter_table(
        cls, table: "agate.Table", used_schemas: FrozenSet[Tuple[str, str]]
    ) -> "agate.Table":
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

    def _show_object_metadata(self, relation: SnowflakeRelation) -> Optional[dict]:
        try:
            kwargs = {"relation": relation}
            results = self.execute_macro(SHOW_OBJECT_METADATA_MACRO_NAME, kwargs=kwargs)

            if len(results) == 0:
                return None

            return results
        except DbtDatabaseError:
            return None

    def get_catalog_for_single_relation(
        self, relation: SnowflakeRelation
    ) -> Optional[CatalogTable]:
        object_metadata = self._show_object_metadata(relation.as_case_sensitive())

        if not object_metadata:
            return None

        row = object_metadata[0]

        is_dynamic = row.get("is_dynamic") in ("Y", "YES")
        kind = row.get("kind")

        if is_dynamic and kind == str(SnowflakeRelationType.Table).upper():
            table_type = str(SnowflakeRelationType.DynamicTable).upper()
        else:
            table_type = kind

        # https://docs.snowflake.com/en/sql-reference/sql/show-views#output
        # Note: we don't support materialized views in dbt-snowflake
        is_view = kind == str(SnowflakeRelationType.View).upper()

        table_metadata = TableMetadata(
            type=table_type,
            schema=row.get("schema_name"),
            name=row.get("name"),
            database=row.get("database_name"),
            comment=row.get("comment"),
            owner=row.get("owner"),
        )

        stats_dict: StatsDict = {
            "has_stats": StatsItem(
                id="has_stats",
                label="Has Stats?",
                value=True,
                include=False,
                description="Indicates whether there are statistics for this table",
            ),
            "row_count": StatsItem(
                id="row_count",
                label="Row Count",
                value=row.get("rows"),
                include=(not is_view),
                description="Number of rows in the table as reported by Snowflake",
            ),
            "bytes": StatsItem(
                id="bytes",
                label="Approximate Size",
                value=row.get("bytes"),
                include=(not is_view),
                description="Size of the table as reported by Snowflake",
            ),
        }

        catalog_columns = {
            c.column: ColumnMetadata(type=c.dtype, index=i + 1, name=c.column)
            for i, c in enumerate(self.get_columns_in_relation(relation))
        }

        return CatalogTable(
            metadata=table_metadata,
            columns=catalog_columns,
            stats=stats_dict,
        )

    def list_relations_without_caching(
        self, schema_relation: SnowflakeRelation
    ) -> List[SnowflakeRelation]:
        kwargs = {"schema_relation": schema_relation}

        try:
            schema_objects = self.execute_macro(LIST_RELATIONS_MACRO_NAME, kwargs=kwargs)
        except DbtDatabaseError as exc:
            # if the schema doesn't exist, we just want to return.
            # Alternatively, we could query the list of schemas before we start
            # and skip listing the missing ones, which sounds expensive.
            if "Object does not exist" in str(exc):
                return []
            raise

        # this can be collapsed once Snowflake adds is_iceberg to show objects
        columns = ["database_name", "schema_name", "name", "kind", "is_dynamic"]
        if self.behavior.enable_iceberg_materializations.no_warn:
            columns.append("is_iceberg")

        return [self._parse_list_relations_result(obj) for obj in schema_objects.select(columns)]

    def _parse_list_relations_result(self, result: "agate.Row") -> SnowflakeRelation:
        # this can be collapsed once Snowflake adds is_iceberg to show objects
        if self.behavior.enable_iceberg_materializations.no_warn:
            database, schema, identifier, relation_type, is_dynamic, is_iceberg = result
        else:
            database, schema, identifier, relation_type, is_dynamic = result
            is_iceberg = "N"

        try:
            relation_type = self.Relation.get_relation_type(relation_type.lower())
        except ValueError:
            relation_type = self.Relation.External

        if relation_type == self.Relation.Table and is_dynamic == "Y":
            relation_type = self.Relation.DynamicTable

        table_format = TableFormat.ICEBERG if is_iceberg in ("Y", "YES") else TableFormat.DEFAULT

        quote_policy = {"database": True, "schema": True, "identifier": True}

        return self.Relation.create(
            database=database,
            schema=schema,
            identifier=identifier,
            type=relation_type,
            table_format=table_format,
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

    GrantsDict = Dict[str, Dict[str, Any]]

    @available
    def standardize_grants_dict(self, grants_table: "agate.Table") -> GrantsDict:
        grants_dict: Dict[str, Any] = {}
        self.AdapterSpecificConfigs

        # granted_to maps to different object types like
        # role, database_role and share
        # create nest dictionaries [granted_to].[privilege][object_name]
        for row in grants_table:
            grantee = row["grantee_name"]
            granted_to = row["granted_to"]
            privilege = row["privilege"]
            if privilege != "OWNERSHIP":
                role_type_dict = grants_dict.setdefault(granted_to.lower(), {})
                privilege_dict = role_type_dict.setdefault(privilege.lower(), [])
                privilege_dict.append(grantee)

        return grants_dict

    @available
    def standardize_grant_config(self, grant_config: dict) -> GrantsDict:
        """
        Given a grants configuration object of either
        Dict[str, Any] or Dict[str, [Dict[str, List[str] ] ]]]

        Converts to Dict[str, Any] to [Dict["role", List[str]] ]]]

        This enables use to handle new and older style grants
        configurations.
        """
        grant_config_std: Dict[str, Any] = {}
        self.AdapterSpecificConfigs

        for grant_config_privilege, privilege_collection in grant_config.items():
            # loop through the role entries and handle mapping, list & string entries
            for privilege_item in privilege_collection:
                # Assume old style list grants map to role
                if not isinstance(privilege_item, dict):
                    privilege_item = {"role": privilege_item}

                for grantee_type, grantees in privilege_item.items():
                    # -- Make sure object_type is in grant_config_by_type
                    grantee_type_privileges: Dict[str, Any] = grant_config_std.setdefault(
                        grantee_type.lower(), {}
                    )
                    privilege_list = grantee_type_privileges.setdefault(
                        grant_config_privilege.lower(), []
                    )

                    # -- convert string to array to make code simpler --#}
                    if isinstance(grantees, str):
                        grantees = [grantees]

                    for grantee in grantees:
                        # -- Only add the item if not already in the list --#}
                        if grantee not in privilege_list:
                            privilege_list.append(grantee)

        return grant_config_std

    @available
    def diff_of_grants(self, grants_a: GrantsDict, grants_b: GrantsDict) -> GrantsDict:
        """
        Given two dictionaries of type Dict[str, Dict[str, List[str]]]:
            grants_a = {'key_x': {'key_a': ['VALUE_1', 'VALUE_2']}, 'KEY_Y': {'key_b': ['value_2']}}
            grants_b = {'KEY_x': {'key_a': ['VALUE_1']}, 'key_y': {'key_b': ['value_3']}}
        Return the same dictionary representation of dict_a MINUS dict_b,
        performing a case-insensitive comparison between the strings in each.
        All keys returned will be in the original case of dict_a.
            returns {"key_x": {'key_a': ['VALUE_2']},"KEY_Y":{"key_b": ["value_2"]}}
        """

        def diff_of_two_dicts(dict_a: dict, dict_b: dict) -> dict:
            """
            Given two dictionaries of type Dict[str, List[str]]:
                dict_a = {'key_x': ['value_1', 'VALUE_2'], 'KEY_Y': ['value_3']}
                dict_b = {'key_x': ['value_1'], 'key_z': ['value_4']}
            Return the same dictionary representation of dict_a MINUS dict_b,
            performing a case-insensitive comparison between the strings in each.
            All keys returned will be in the original case of dict_a.
                returns {'key_x': ['VALUE_2'], 'KEY_Y': ['value_3']}
            """

            dict_diff = {}
            dict_b_lowered = {k.casefold(): [x.casefold() for x in v] for k, v in dict_b.items()}
            for k in dict_a:
                if k.casefold() in dict_b_lowered.keys():
                    diff = []
                    for v in dict_a[k]:
                        if v.casefold() not in dict_b_lowered[k.casefold()]:
                            diff.append(v)
                    if diff:
                        dict_diff.update({k: diff})
                else:
                    dict_diff.update({k: dict_a[k]})
            return dict_diff

        grants_diff: Dict[str, Any] = {}
        grants_b_lowered = {
            k.casefold(): {x.casefold(): y for x, y in v.items()} for k, v in grants_b.items()
        }
        for k in grants_a:
            if k.casefold() in grants_b_lowered.keys():
                diff = diff_of_two_dicts(grants_a[k], grants_b_lowered[k.casefold()])
                if diff:
                    grants_diff.update({k: diff})
            else:
                grants_diff.update({k: grants_a[k]})

        return grants_diff

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
        return ["append", "merge", "delete+insert", "microbatch"]

    def debug_query(self):
        """Override for DebugTask method"""
        self.execute("select 1 as id")
