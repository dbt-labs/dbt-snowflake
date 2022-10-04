import base64
import datetime
import pytz
import re
from contextlib import contextmanager
from dataclasses import dataclass
from io import StringIO
from time import sleep
from typing import Optional, Tuple

import agate
import dbt.clients.agate_helper

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import requests
import snowflake.connector
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

from dbt.exceptions import (
    InternalException,
    RuntimeException,
    FailedToConnectException,
    DatabaseException,
    warn_or_error,
)
from dbt.adapters.base import Credentials  # type: ignore
from dbt.contracts.connection import AdapterResponse
from dbt.adapters.sql import SQLConnectionManager  # type: ignore
from dbt.events import AdapterLogger  # type: ignore


logger = AdapterLogger("Snowflake")
_TOKEN_REQUEST_URL = "https://{}.snowflakecomputing.com/oauth/token-request"


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

    def __post_init__(self):
        if self.authenticator != "oauth" and (
            self.oauth_client_secret or self.oauth_client_id or self.token
        ):
            # the user probably forgot to set 'authenticator' like I keep doing
            warn_or_error(
                "Authenticator is not set to oauth, but an oauth-only "
                "parameter is set! Did you mean to set authenticator: oauth?"
            )

    @property
    def type(self):
        return "snowflake"

    @property
    def unique_field(self):
        return self.account

    def _connection_keys(self):
        return (
            "account",
            "user",
            "database",
            "schema",
            "warehouse",
            "role",
            "client_session_keep_alive",
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
                        "Invalid profile: got an oauth_client_id, but not an "
                        "oauth_client_secret!"
                    )
                elif self.oauth_client_secret:
                    warn_or_error(
                        "Invalid profile: got an oauth_client_secret, but not "
                        "an oauth_client_id!"
                    )

                result["token"] = token
            # enable id token cache for linux
            result["client_store_temporary_credential"] = True
            # enable mfa token cache for linux
            result["client_request_mfa_token"] = True
        result["private_key"] = self._get_private_key()
        return result

    def _get_access_token(self) -> str:
        if self.authenticator != "oauth":
            raise InternalException("Can only get access tokens for oauth")
        missing = any(
            x is None for x in (self.oauth_client_id, self.oauth_client_secret, self.token)
        )
        if missing:
            raise InternalException(
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
            raise DatabaseException(
                f"""Did not receive valid json with access_token.
                                        Showing json response: {result_json}"""
            )

        return result_json["access_token"]

    def _get_private_key(self):
        """Get Snowflake private key by path or None."""
        if not self.private_key_path:
            return None

        if self.private_key_passphrase:
            encoded_passphrase = self.private_key_passphrase.encode()
        else:
            encoded_passphrase = None

        with open(self.private_key_path, "rb") as key:
            p_key = serialization.load_pem_private_key(
                key.read(), password=encoded_passphrase, backend=default_backend()
            )

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

            logger.debug("Snowflake query id: {}".format(e.sfqid))
            logger.debug("Snowflake error: {}".format(msg))

            if "Empty SQL statement" in msg:
                logger.debug("got empty sql statement, moving on")
            elif "This session does not have a current database" in msg:
                raise FailedToConnectException(
                    (
                        "{}\n\nThis error sometimes occurs when invalid "
                        "credentials are provided, or when your default role "
                        "does not have access to use the specified database. "
                        "Please double check your profile and try again."
                    ).format(msg)
                )
            else:
                raise DatabaseException(msg)
        except Exception as e:
            if isinstance(e, snowflake.connector.errors.Error):
                logger.debug("Snowflake query id: {}".format(e.sfqid))

            logger.debug("Error running SQL: {}", sql)
            logger.debug("Rolling back transaction.")
            self.rollback_if_open()
            if isinstance(e, RuntimeException):
                # during a sql query, an internal to dbt exception was raised.
                # this sounds a lot like a signal handler and probably has
                # useful information, so raise it without modification.
                raise
            raise RuntimeException(str(e)) from e

    @classmethod
    def open(cls, connection):
        if connection.state == "open":
            logger.debug("Connection is already open, skipping open.")
            return connection

        creds = connection.credentials
        timeout = creds.connect_timeout

        def connect():
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
                **creds.auth_args(),
            )

            if creds.query_tag:
                handle.cursor().execute(
                    ("alter session set query_tag = '{}'").format(creds.query_tag)
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

        sql = "select system$abort_session({})".format(sid)

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
        self, sql: str, auto_begin: bool = False, fetch: bool = False
    ) -> Tuple[AdapterResponse, agate.Table]:
        # don't apply the query comment here
        # it will be applied after ';' queries are split
        _, cursor = self.add_query(sql, auto_begin)
        response = self.get_response(cursor)
        if fetch:
            table = self.get_result_from_cursor(cursor)
        else:
            table = dbt.clients.agate_helper.empty_table()
        return response, table

    def add_query(self, sql, auto_begin=True, bindings=None, abridge_sql_log=False):

        connection = None
        cursor = None

        if bindings:
            # The snowflake connector is more strict than, eg., psycopg2 -
            # which allows any iterable thing to be passed as a binding.
            bindings = tuple(bindings)

        queries = self._split_queries(sql)

        for individual_query in queries:
            # hack -- after the last ';', remove comments and don't run
            # empty queries. this avoids using exceptions as flow control,
            # and also allows us to return the status of the last cursor
            without_comments = re.sub(
                re.compile(r"(\".*?\"|\'.*?\')|(/\*.*?\*/|--[^\r\n]*$)", re.MULTILINE),
                "",
                individual_query,
            ).strip()

            if without_comments == "":
                continue

            # Even though we turn off transactions by default for Snowflake,
            # the user/macro has passed them *explicitly*, probably to wrap a DML statement
            # Let their wish be granted!
            # This also has the effect of ignoring "commit" in the RunResult for this model
            # https://github.com/dbt-labs/dbt-snowflake/issues/147
            if individual_query.lower() == "begin;":
                super().add_begin_query()
                continue

            elif individual_query.lower() == "commit;":
                super().add_commit_query()
                continue

            # add a query comment to *every* statement
            # needed for models with multi-step materializations
            # https://github.com/dbt-labs/dbt-snowflake/issues/140
            individual_query = self._add_query_comment(individual_query)

            connection, cursor = super().add_query(
                individual_query, auto_begin, bindings=bindings, abridge_sql_log=abridge_sql_log
            )

        if cursor is None:
            conn = self.get_thread_connection()
            if conn is None or conn.name is None:
                conn_name = "<None>"
            else:
                conn_name = conn.name

            raise RuntimeException(
                "Tried to run an empty query on model '{}'. If you are "
                "conditionally running\nsql, eg. in a model hook, make "
                "sure your `else` clause contains valid sql!\n\n"
                "Provided SQL:\n{}".format(conn_name, sql)
            )

        return connection, cursor
