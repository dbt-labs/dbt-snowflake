import base64
import datetime
import os

import pytz
import re
from contextlib import contextmanager
from dataclasses import dataclass
from io import StringIO
from time import sleep
from typing import Optional, Tuple, Union, Any, List

import agate
from dbt_common.clients.agate_helper import empty_table

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import requests
import snowflake.connector
import snowflake.connector.constants
import snowflake.connector.errors
from snowflake.connector.errors import (
    Error,
    DatabaseError,
    InternalError,
    InternalServerError,
    ServiceUnavailableError,
    GatewayTimeoutError,
    RequestTimeoutError,
    BadGatewayError,
    OtherHTTPRetryableError,
    BindUploadError,
)

from dbt_common.exceptions import (
    DbtInternalError,
    DbtRuntimeError,
    DbtConfigError,
)
from dbt_common.exceptions import DbtDatabaseError
from dbt.adapters.exceptions.connection import FailedToConnectError
from dbt.adapters.contracts.connection import AdapterResponse, Connection, Credentials
from dbt.adapters.sql import SQLConnectionManager  # type: ignore
from dbt.adapters.events.logging import AdapterLogger  # type: ignore
from dbt_common.events.functions import warn_or_error
from dbt.adapters.events.types import AdapterEventWarning
from dbt_common.ui import line_wrap_message, warning_tag


logger = AdapterLogger("Snowflake")

if os.getenv("DBT_SNOWFLAKE_CONNECTOR_DEBUG_LOGGING"):
    for logger_name in ["snowflake.connector", "botocore", "boto3"]:
        logger.debug(f"Setting {logger_name} to DEBUG")
        logger.set_adapter_dependency_log_level(logger_name, "DEBUG")

_TOKEN_REQUEST_URL = "https://{}.snowflakecomputing.com/oauth/token-request"

ERROR_REDACTION_PATTERNS = {
    re.compile(r"Row Values: \[(.|\n)*\]"): "Row Values: [redacted]",
    re.compile(r"Duplicate field key '(.|\n)*'"): "Duplicate field key '[redacted]'",
}


@dataclass
class SnowflakeAdapterResponse(AdapterResponse):
    query_id: str = ""


@dataclass
class SnowflakeCredentials(Credentials):
    account: str
    user: str
    warehouse: Optional[str] = None
    role: Optional[str] = None
    password: Optional[str] = None
    authenticator: Optional[str] = None
    private_key: Optional[str] = None
    private_key_path: Optional[str] = None
    private_key_passphrase: Optional[str] = None
    token: Optional[str] = None
    oauth_client_id: Optional[str] = None
    oauth_client_secret: Optional[str] = None
    query_tag: Optional[str] = None
    client_session_keep_alive: bool = False
    host: Optional[str] = None
    port: Optional[int] = None
    proxy_host: Optional[str] = None
    proxy_port: Optional[int] = None
    protocol: Optional[str] = None
    connect_retries: int = 1
    connect_timeout: Optional[int] = None
    retry_on_database_errors: bool = False
    retry_all: bool = False
    insecure_mode: Optional[bool] = False
    reuse_connections: Optional[bool] = None

    def __post_init__(self):
        if self.authenticator != "oauth" and (
            self.oauth_client_secret or self.oauth_client_id or self.token
        ):
            # the user probably forgot to set 'authenticator' like I keep doing
            warn_or_error(
                AdapterEventWarning(
                    base_msg="Authenticator is not set to oauth, but an oauth-only parameter is set! Did you mean to set authenticator: oauth?"
                )
            )

    @property
    def type(self):
        return "snowflake"

    @property
    def unique_field(self):
        return self.account

    # the results show up in the output of dbt debug runs, for more see..
    # https://docs.getdbt.com/guides/dbt-ecosystem/adapter-development/3-building-a-new-adapter#editing-the-connection-manager
    def _connection_keys(self):
        return (
            "account",
            "user",
            "database",
            "warehouse",
            "role",
            "schema",
            "authenticator",
            "oauth_client_id",
            "query_tag",
            "client_session_keep_alive",
            "host",
            "port",
            "proxy_host",
            "proxy_port",
            "protocol",
            "connect_retries",
            "connect_timeout",
            "retry_on_database_errors",
            "retry_all",
            "insecure_mode",
            "reuse_connections",
        )

    def auth_args(self):
        # Pull all of the optional authentication args for the connector,
        # let connector handle the actual arg validation
        result = {}
        if self.password:
            result["password"] = self.password
        if self.host:
            result["host"] = self.host
        if self.port:
            result["port"] = self.port
        if self.proxy_host:
            result["proxy_host"] = self.proxy_host
        if self.proxy_port:
            result["proxy_port"] = self.proxy_port
        if self.protocol:
            result["protocol"] = self.protocol
        if self.authenticator:
            result["authenticator"] = self.authenticator
            if self.authenticator == "oauth":
                token = self.token
                # if we have a client ID/client secret, the token is a refresh
                # token, not an access token
                if self.oauth_client_id and self.oauth_client_secret:
                    token = self._get_access_token()
                elif self.oauth_client_id:
                    warn_or_error(
                        AdapterEventWarning(
                            base_msg="Invalid profile: got an oauth_client_id, but not an oauth_client_secret!"
                        )
                    )
                elif self.oauth_client_secret:
                    warn_or_error(
                        AdapterEventWarning(
                            base_msg="Invalid profile: got an oauth_client_secret, but not an oauth_client_id!"
                        )
                    )

                result["token"] = token
            # enable id token cache for linux
            result["client_store_temporary_credential"] = True
            # enable mfa token cache for linux
            result["client_request_mfa_token"] = True
        result["reuse_connections"] = self.reuse_connections
        result["private_key"] = self._get_private_key()
        return result

    def _get_access_token(self) -> str:
        if self.authenticator != "oauth":
            raise DbtInternalError("Can only get access tokens for oauth")
        missing = any(
            x is None for x in (self.oauth_client_id, self.oauth_client_secret, self.token)
        )
        if missing:
            raise DbtInternalError(
                "need a client ID a client secret, and a refresh token to get " "an access token"
            )

        # should the full url be a config item?
        token_url = _TOKEN_REQUEST_URL.format(self.account)
        # I think this is only used to redirect on success, which we ignore
        # (it does not have to match the integration's settings in snowflake)
        redirect_uri = "http://localhost:9999"
        data = {
            "grant_type": "refresh_token",
            "refresh_token": self.token,
            "redirect_uri": redirect_uri,
        }

        auth = base64.b64encode(
            f"{self.oauth_client_id}:{self.oauth_client_secret}".encode("ascii")
        ).decode("ascii")
        headers = {
            "Authorization": f"Basic {auth}",
            "Content-type": "application/x-www-form-urlencoded;charset=utf-8",
        }

        result_json = None
        max_iter = 20
        # Attempt to obtain JSON for 1 second before throwing an error
        for i in range(max_iter):
            result = requests.post(token_url, headers=headers, data=data)
            try:
                result_json = result.json()
                break
            except ValueError as e:
                message = result.text
                logger.debug(
                    f"Got a non-json response ({result.status_code}): \
                              {e}, message: {message}"
                )
                sleep(0.05)

        if result_json is None:
            raise DbtDatabaseError(
                f"""Did not receive valid json with access_token.
                                        Showing json response: {result_json}"""
            )

        return result_json["access_token"]

    def _get_private_key(self):
        """Get Snowflake private key by path, from a Base64 encoded DER bytestring or None."""
        if self.private_key and self.private_key_path:
            raise DbtConfigError("Cannot specify both `private_key`  and `private_key_path`")

        if self.private_key_passphrase:
            encoded_passphrase = self.private_key_passphrase.encode()
        else:
            encoded_passphrase = None

        if self.private_key:
            if self.private_key.startswith("-"):
                p_key = serialization.load_pem_private_key(
                    data=bytes(self.private_key, "utf-8"),
                    password=encoded_passphrase,
                    backend=default_backend(),
                )

            else:
                p_key = serialization.load_der_private_key(
                    data=base64.b64decode(self.private_key),
                    password=encoded_passphrase,
                    backend=default_backend(),
                )

        elif self.private_key_path:
            with open(self.private_key_path, "rb") as key:
                p_key = serialization.load_pem_private_key(
                    key.read(), password=encoded_passphrase, backend=default_backend()
                )
        else:
            return None

        return p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )


class SnowflakeConnectionManager(SQLConnectionManager):
    TYPE = "snowflake"

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield
        except snowflake.connector.errors.ProgrammingError as e:
            msg = str(e)

            # A class of Snowflake errors -- such as a failure from attempting to merge
            # duplicate rows -- includes row values in the error message, i.e.
            # [12345, "col_a_value", "col_b_value", etc...]. We don't want to log potentially
            # sensitive user data.
            for regex_pattern, replacement_message in ERROR_REDACTION_PATTERNS.items():
                msg = re.sub(regex_pattern, replacement_message, msg)

            logger.debug("Snowflake query id: {}".format(e.sfqid))
            logger.debug("Snowflake error: {}".format(msg))

            if "Empty SQL statement" in msg:
                logger.debug("got empty sql statement, moving on")
            elif "This session does not have a current database" in msg:
                raise FailedToConnectError(
                    (
                        "{}\n\nThis error sometimes occurs when invalid "
                        "credentials are provided, or when your default role "
                        "does not have access to use the specified database. "
                        "Please double check your profile and try again."
                    ).format(msg)
                )
            else:
                raise DbtDatabaseError(msg)
        except Exception as e:
            if isinstance(e, snowflake.connector.errors.Error):
                logger.debug("Snowflake query id: {}".format(e.sfqid))

            logger.debug("Error running SQL: {}", sql)
            logger.debug("Rolling back transaction.")
            self.rollback_if_open()
            if isinstance(e, DbtRuntimeError):
                # during a sql query, an internal to dbt exception was raised.
                # this sounds a lot like a signal handler and probably has
                # useful information, so raise it without modification.
                raise
            raise DbtRuntimeError(str(e)) from e

    @classmethod
    def open(cls, connection):
        if connection.state == "open":
            logger.debug("Connection is already open, skipping open.")
            return connection

        creds = connection.credentials
        timeout = creds.connect_timeout

        def connect():
            session_parameters = {}

            if creds.query_tag:
                session_parameters.update({"QUERY_TAG": creds.query_tag})

            handle = snowflake.connector.connect(
                account=creds.account,
                user=creds.user,
                database=creds.database,
                schema=creds.schema,
                warehouse=creds.warehouse,
                role=creds.role,
                autocommit=True,
                client_session_keep_alive=creds.client_session_keep_alive,
                application="dbt",
                insecure_mode=creds.insecure_mode,
                session_parameters=session_parameters,
                **creds.auth_args(),
            )

            return handle

        def exponential_backoff(attempt: int):
            return attempt * attempt

        retryable_exceptions = [
            InternalError,
            InternalServerError,
            ServiceUnavailableError,
            GatewayTimeoutError,
            RequestTimeoutError,
            BadGatewayError,
            OtherHTTPRetryableError,
            BindUploadError,
        ]
        # these two options are for backwards compatibility
        if creds.retry_all:
            retryable_exceptions = [Error]
        elif creds.retry_on_database_errors:
            retryable_exceptions.insert(0, DatabaseError)

        return cls.retry_connection(
            connection,
            connect=connect,
            logger=logger,
            retry_limit=creds.connect_retries,
            retry_timeout=timeout if timeout is not None else exponential_backoff,
            retryable_exceptions=retryable_exceptions,
        )

    def cancel(self, connection):
        handle = connection.handle
        sid = handle.session_id

        connection_name = connection.name

        sql = "select system$cancel_all_queries({})".format(sid)

        logger.debug("Cancelling query '{}' ({})".format(connection_name, sid))

        _, cursor = self.add_query(sql)
        res = cursor.fetchone()

        logger.debug("Cancel query '{}': {}".format(connection_name, res))

    @classmethod
    def get_response(cls, cursor) -> SnowflakeAdapterResponse:
        code = cursor.sqlstate

        if code is None:
            code = "SUCCESS"

        return SnowflakeAdapterResponse(
            _message="{} {}".format(code, cursor.rowcount),
            rows_affected=cursor.rowcount,
            code=code,
            query_id=cursor.sfqid,
        )  # type: ignore

    # disable transactional logic by default on Snowflake
    # except for DML statements where explicitly defined
    def add_begin_query(self, *args, **kwargs):
        pass

    def add_commit_query(self, *args, **kwargs):
        pass

    def begin(self):
        pass

    def commit(self):
        pass

    def clear_transaction(self):
        pass

    @classmethod
    def _split_queries(cls, sql):
        "Splits sql statements at semicolons into discrete queries"

        sql_s = str(sql)
        sql_buf = StringIO(sql_s)
        split_query = snowflake.connector.util_text.split_statements(sql_buf)
        return [part[0] for part in split_query]

    @classmethod
    def process_results(cls, column_names, rows):
        # Override for Snowflake. The datetime objects returned by
        # snowflake-connector-python are not pickleable, so we need
        # to replace them with sane timezones
        fixed = []
        for row in rows:
            fixed_row = []
            for col in row:
                if isinstance(col, datetime.datetime) and col.tzinfo:
                    offset = col.utcoffset()
                    offset_seconds = offset.total_seconds()
                    new_timezone = pytz.FixedOffset(offset_seconds // 60)
                    col = col.astimezone(tz=new_timezone)
                fixed_row.append(col)

            fixed.append(fixed_row)

        return super().process_results(column_names, fixed)

    def execute(
        self, sql: str, auto_begin: bool = False, fetch: bool = False, limit: Optional[int] = None
    ) -> Tuple[AdapterResponse, agate.Table]:
        # don't apply the query comment here
        # it will be applied after ';' queries are split
        _, cursor = self.add_query(sql, auto_begin)
        response = self.get_response(cursor)
        if fetch:
            table = self.get_result_from_cursor(cursor, limit)
        else:
            table = empty_table()
        return response, table

    def add_standard_query(self, sql: str, **kwargs) -> Tuple[Connection, Any]:
        # This is the happy path for a single query. Snowflake has a few odd behaviors that
        # require preprocessing within the 'add_query' method below.
        return super().add_query(self._add_query_comment(sql), **kwargs)

    def add_query(
        self,
        sql: str,
        auto_begin: bool = True,
        bindings: Optional[Any] = None,
        abridge_sql_log: bool = False,
    ) -> Tuple[Connection, Any]:  # type: ignore
        if bindings:
            # The snowflake connector is stricter than, e.g., psycopg2 -
            # which allows any iterable thing to be passed as a binding.
            bindings = tuple(bindings)

        stripped_queries = self._stripped_queries(sql)

        if set(query.lower() for query in stripped_queries).issubset({"begin;", "commit;"}):
            connection, cursor = self._add_begin_commit_only_queries(
                stripped_queries,
                auto_begin=auto_begin,
                bindings=bindings,
                abridge_sql_log=abridge_sql_log,
            )
        else:
            connection, cursor = self._add_standard_queries(
                stripped_queries,
                auto_begin=auto_begin,
                bindings=bindings,
                abridge_sql_log=abridge_sql_log,
            )

        if cursor is None:
            self._raise_cursor_not_found_error(sql)

        return connection, cursor  # type: ignore

    def _stripped_queries(self, sql: str) -> List[str]:
        def strip_query(query):
            """
            hack -- after the last ';', remove comments and don't run
            empty queries. this avoids using exceptions as flow control,
            and also allows us to return the status of the last cursor
            """
            without_comments_re = re.compile(
                r"(\".*?\"|\'.*?\')|(/\*.*?\*/|--[^\r\n]*$)", re.MULTILINE
            )
            return re.sub(without_comments_re, "", query).strip()

        return [query for query in self._split_queries(sql) if strip_query(query) != ""]

    def _add_begin_commit_only_queries(
        self, queries: List[str], **kwargs
    ) -> Tuple[Connection, Any]:
        # if all we get is `begin;` and/or `commit;`
        # raise a warning, then run as standard queries to avoid an error downstream
        message = (
            "Explicit transactional logic should be used only to wrap "
            "DML logic (MERGE, DELETE, UPDATE, etc). The keywords BEGIN; and COMMIT; should "
            "be placed directly before and after your DML statement, rather than in separate "
            "statement calls or run_query() macros."
        )
        logger.warning(line_wrap_message(warning_tag(message)))

        for query in queries:
            connection, cursor = self.add_standard_query(query, **kwargs)
        return connection, cursor

    def _add_standard_queries(self, queries: List[str], **kwargs) -> Tuple[Connection, Any]:
        for query in queries:
            # Even though we turn off transactions by default for Snowflake,
            # the user/macro has passed them *explicitly*, probably to wrap a DML statement
            # This also has the effect of ignoring "commit" in the RunResult for this model
            # https://github.com/dbt-labs/dbt-snowflake/issues/147
            if query.lower() == "begin;":
                super().add_begin_query()
            elif query.lower() == "commit;":
                super().add_commit_query()
            else:
                # This adds a query comment to *every* statement
                # https://github.com/dbt-labs/dbt-snowflake/issues/140
                connection, cursor = self.add_standard_query(query, **kwargs)
        return connection, cursor

    def _raise_cursor_not_found_error(self, sql: str):
        conn = self.get_thread_connection()
        try:
            conn_name = conn.name
        except AttributeError:
            conn_name = None

        raise DbtRuntimeError(
            f"""Tried to run an empty query on model '{conn_name or "<None>"}'. If you are """
            f"""conditionally running\nsql, e.g. in a model hook, make """
            f"""sure your `else` clause contains valid sql!\n\n"""
            f"""Provided SQL:\n{sql}"""
        )

    def release(self):
        """Reuse connections by deferring release until adapter context manager in core
        resets adapters. This cleanup_all happens before Python teardown. Idle connections
        incur no costs while waiting in the connection pool."""
        if self.profile.credentials.reuse_connections:  # type: ignore
            return
        super().release()

    @classmethod
    def data_type_code_to_name(cls, type_code: Union[int, str]) -> str:
        assert isinstance(type_code, int)
        return snowflake.connector.constants.FIELD_ID_TO_NAME[type_code]
