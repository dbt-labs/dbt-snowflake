import agate
import re
import unittest
from multiprocessing import get_context
from contextlib import contextmanager
from unittest import mock

from dbt.adapters.snowflake import SnowflakeAdapter
from dbt.adapters.snowflake import Plugin as SnowflakePlugin
from dbt.adapters.snowflake.column import SnowflakeColumn
from dbt.adapters.snowflake.connections import SnowflakeCredentials
from dbt.contracts.files import FileHash
from dbt.context.query_header import generate_query_header_context
from dbt.context.providers import generate_runtime_macro_context
from dbt.contracts.graph.manifest import ManifestStateCheck
from dbt_common.clients import agate_helper
from snowflake import connector as snowflake_connector

from .utils import (
    config_from_parts_or_dicts,
    inject_adapter,
    mock_connection,
    TestAdapterConversions,
    load_internal_manifest_macros,
)


class TestSnowflakeAdapter(unittest.TestCase):
    def setUp(self):
        profile_cfg = {
            "outputs": {
                "test": {
                    "type": "snowflake",
                    "account": "test_account",
                    "user": "test_user",
                    "database": "test_database",
                    "warehouse": "test_warehouse",
                    "schema": "public",
                },
            },
            "target": "test",
        }

        project_cfg = {
            "name": "X",
            "version": "0.1",
            "profile": "test",
            "project-root": "/tmp/dbt/does-not-exist",
            "quoting": {
                "identifier": False,
                "schema": True,
            },
            "query-comment": "dbt",
            "config-version": 2,
        }
        self.config = config_from_parts_or_dicts(project_cfg, profile_cfg)
        self.assertEqual(self.config.query_comment.comment, "dbt")
        self.assertEqual(self.config.query_comment.append, False)

        self.handle = mock.MagicMock(spec=snowflake_connector.SnowflakeConnection)
        self.cursor = self.handle.cursor.return_value
        self.mock_execute = self.cursor.execute
        self.mock_execute.return_value = mock.MagicMock(sfqid="42")
        self.patcher = mock.patch("dbt.adapters.snowflake.connections.snowflake.connector.connect")
        self.snowflake = self.patcher.start()
        self.snowflake.connect.cursor.return_value = mock.MagicMock(sfqid="42")

        # Create the Manifest.state_check patcher
        @mock.patch("dbt.parser.manifest.ManifestLoader.build_manifest_state_check")
        def _mock_state_check(self):
            all_projects = self.all_projects
            return ManifestStateCheck(
                vars_hash=FileHash.from_contents("vars"),
                project_hashes={name: FileHash.from_contents(name) for name in all_projects},
                profile_hash=FileHash.from_contents("profile"),
            )

        self.load_state_check = mock.patch(
            "dbt.parser.manifest.ManifestLoader.build_manifest_state_check"
        )
        self.mock_state_check = self.load_state_check.start()
        self.mock_state_check.side_effect = _mock_state_check

        self.snowflake.return_value = self.handle
        self.adapter = SnowflakeAdapter(self.config, get_context("spawn"))
        self.adapter.set_macro_resolver(load_internal_manifest_macros(self.config))
        self.adapter.set_macro_context_generator(generate_runtime_macro_context)
        self.adapter.connections.set_query_header(
            generate_query_header_context(self.config, self.adapter.get_macro_resolver())
        )

        self.qh_patch = mock.patch.object(self.adapter.connections.query_header, "add")
        self.mock_query_header_add = self.qh_patch.start()
        self.mock_query_header_add.side_effect = lambda q: "/* dbt */\n{}".format(q)
        self.adapter.acquire_connection()
        inject_adapter(self.adapter, SnowflakePlugin)

    def tearDown(self):
        # we want a unique self.handle every time.
        self.adapter.cleanup_connections()
        self.qh_patch.stop()
        self.patcher.stop()
        self.load_state_check.stop()

    def test_quoting_on_drop_schema(self):
        relation = SnowflakeAdapter.Relation.create(
            database="test_database",
            schema="test_schema",
            quote_policy=self.adapter.config.quoting,
        )
        self.adapter.drop_schema(relation)

        self.mock_execute.assert_has_calls(
            [
                mock.call(
                    '/* dbt */\ndrop schema if exists test_database."test_schema" cascade', None
                )
            ]
        )

    def test_quoting_on_drop(self):
        relation = self.adapter.Relation.create(
            database="test_database",
            schema="test_schema",
            identifier="test_table",
            type="table",
            quote_policy=self.adapter.config.quoting,
        )
        self.adapter.drop_relation(relation)

        self.mock_execute.assert_has_calls(
            [
                mock.call(
                    '/* dbt */\ndrop table if exists test_database."test_schema".test_table cascade',
                    None,
                )
            ]
        )

    def test_quoting_on_truncate(self):
        relation = self.adapter.Relation.create(
            database="test_database",
            schema="test_schema",
            identifier="test_table",
            type="table",
            quote_policy=self.adapter.config.quoting,
        )
        self.adapter.truncate_relation(relation)

        # no query comment because wrapped in begin; + commit; for explicit DML
        self.mock_execute.assert_has_calls(
            [
                mock.call("/* dbt */\nBEGIN", None),
                mock.call(
                    '/* dbt */\ntruncate table test_database."test_schema".test_table\n  ;', None
                ),
                mock.call("/* dbt */\nCOMMIT", None),
            ]
        )

    def test_quoting_on_rename(self):
        from_relation = self.adapter.Relation.create(
            database="test_database",
            schema="test_schema",
            identifier="table_a",
            type="table",
            quote_policy=self.adapter.config.quoting,
        )
        to_relation = self.adapter.Relation.create(
            database="test_database",
            schema="test_schema",
            identifier="table_b",
            type="table",
            quote_policy=self.adapter.config.quoting,
        )

        self.adapter.rename_relation(from_relation=from_relation, to_relation=to_relation)
        self.mock_execute.assert_has_calls(
            [
                mock.call(
                    '/* dbt */\nalter table test_database."test_schema".table_a rename to test_database."test_schema".table_b',
                    None,
                )
            ]
        )

    @contextmanager
    def current_warehouse(self, response):
        # there is probably some elegant way built into mock.patch to do this
        fetchall_return = self.cursor.fetchall.return_value
        execute_side_effect = self.mock_execute.side_effect

        def execute_effect(sql, *args, **kwargs):
            if sql == "/* dbt */\nselect current_warehouse() as warehouse":
                self.cursor.description = [["name"]]
                self.cursor.fetchall.return_value = [[response]]
            else:
                self.cursor.description = None
                self.cursor.fetchall.return_value = fetchall_return
            return self.mock_execute.return_value

        self.mock_execute.side_effect = execute_effect
        try:
            yield
        finally:
            self.cursor.fetchall.return_value = fetchall_return
            self.mock_execute.side_effect = execute_side_effect

    def _strip_transactions(self):
        result = []
        for call_args in self.mock_execute.call_args_list:
            args, kwargs = tuple(call_args)
            is_transactional = (
                len(kwargs) == 0
                and len(args) == 2
                and args[1] is None
                and args[0] in {"BEGIN", "COMMIT"}
            )
            if not is_transactional:
                result.append(call_args)
        return result

    def test_pre_post_hooks_warehouse(self):
        with self.current_warehouse("warehouse"):
            config = {"snowflake_warehouse": "other_warehouse"}
            result = self.adapter.pre_model_hook(config)
            self.assertIsNotNone(result)
            calls = [
                mock.call("/* dbt */\nselect current_warehouse() as warehouse", None),
                mock.call("/* dbt */\nuse warehouse other_warehouse", None),
            ]
            self.mock_execute.assert_has_calls(calls)
            self.adapter.post_model_hook(config, result)
            calls.append(mock.call("/* dbt */\nuse warehouse warehouse", None))
            self.mock_execute.assert_has_calls(calls)

    def test_pre_post_hooks_no_warehouse(self):
        with self.current_warehouse("warehouse"):
            config = {}
            result = self.adapter.pre_model_hook(config)
            self.assertIsNone(result)
            self.mock_execute.assert_not_called()
            self.adapter.post_model_hook(config, result)
            self.mock_execute.assert_not_called()

    def test_cancel_open_connections_empty(self):
        self.assertEqual(len(list(self.adapter.cancel_open_connections())), 0)

    def test_cancel_open_connections_master(self):
        key = self.adapter.connections.get_thread_identifier()
        self.adapter.connections.thread_connections[key] = mock_connection("master")
        self.assertEqual(len(list(self.adapter.cancel_open_connections())), 0)

    def test_cancel_open_connections_single(self):
        master = mock_connection("master")
        model = mock_connection("model")
        model.handle.session_id = 42

        key = self.adapter.connections.get_thread_identifier()
        self.adapter.connections.thread_connections.update(
            {
                key: master,
                1: model,
            }
        )
        with mock.patch.object(self.adapter.connections, "add_query") as add_query:
            query_result = mock.MagicMock()
            add_query.return_value = (None, query_result)

            self.assertEqual(len(list(self.adapter.cancel_open_connections())), 1)

            add_query.assert_called_once_with("select system$cancel_all_queries(42)")

    def test_client_session_keep_alive_false_by_default(self):
        conn = self.adapter.connections.set_connection_name(name="new_connection_with_new_config")

        self.snowflake.assert_not_called()
        conn.handle
        self.snowflake.assert_has_calls(
            [
                mock.call(
                    account="test-account",
                    autocommit=True,
                    client_session_keep_alive=False,
                    database="test_database",
                    role=None,
                    schema="public",
                    user="test_user",
                    warehouse="test_warehouse",
                    private_key=None,
                    application="dbt",
                    insecure_mode=False,
                    session_parameters={},
                    reuse_connections=True,
                ),
            ]
        )

    def test_client_session_keep_alive_true(self):
        self.config.credentials = self.config.credentials.replace(
            client_session_keep_alive=True,
            # this gets defaulted via `__post_init__` when `client_session_keep_alive` comes in as `False`
            # then when `replace` is called, `__post_init__` cannot set it back to `None` since it cannot
            # tell the difference between set by user and set by `__post_init__`
            reuse_connections=None,
        )
        self.adapter = SnowflakeAdapter(self.config, get_context("spawn"))
        conn = self.adapter.connections.set_connection_name(name="new_connection_with_new_config")

        self.snowflake.assert_not_called()
        conn.handle
        self.snowflake.assert_has_calls(
            [
                mock.call(
                    account="test-account",
                    autocommit=True,
                    client_session_keep_alive=True,
                    database="test_database",
                    role=None,
                    schema="public",
                    user="test_user",
                    warehouse="test_warehouse",
                    private_key=None,
                    application="dbt",
                    insecure_mode=False,
                    session_parameters={},
                    reuse_connections=None,
                )
            ]
        )

    def test_client_has_query_tag(self):
        self.config.credentials = self.config.credentials.replace(query_tag="test_query_tag")
        self.adapter = SnowflakeAdapter(self.config, get_context("spawn"))
        conn = self.adapter.connections.set_connection_name(name="new_connection_with_new_config")

        self.snowflake.assert_not_called()
        conn.handle
        self.snowflake.assert_has_calls(
            [
                mock.call(
                    account="test-account",
                    autocommit=True,
                    client_session_keep_alive=False,
                    database="test_database",
                    role=None,
                    schema="public",
                    user="test_user",
                    reuse_connections=True,
                    warehouse="test_warehouse",
                    private_key=None,
                    application="dbt",
                    insecure_mode=False,
                    session_parameters={"QUERY_TAG": "test_query_tag"},
                )
            ]
        )

        expected_connection_info = [
            (k, v) for (k, v) in self.config.credentials.connection_info() if k == "query_tag"
        ]
        self.assertEqual([("query_tag", "test_query_tag")], expected_connection_info)

    def test_user_pass_authentication(self):
        self.config.credentials = self.config.credentials.replace(
            password="test_password",
        )
        self.adapter = SnowflakeAdapter(self.config, get_context("spawn"))
        conn = self.adapter.connections.set_connection_name(name="new_connection_with_new_config")

        self.snowflake.assert_not_called()
        conn.handle
        self.snowflake.assert_has_calls(
            [
                mock.call(
                    account="test-account",
                    autocommit=True,
                    client_session_keep_alive=False,
                    database="test_database",
                    password="test_password",
                    role=None,
                    schema="public",
                    user="test_user",
                    warehouse="test_warehouse",
                    private_key=None,
                    application="dbt",
                    insecure_mode=False,
                    session_parameters={},
                    reuse_connections=True,
                )
            ]
        )

    def test_authenticator_user_pass_authentication(self):
        self.config.credentials = self.config.credentials.replace(
            password="test_password",
            authenticator="test_sso_url",
        )
        self.adapter = SnowflakeAdapter(self.config, get_context("spawn"))
        conn = self.adapter.connections.set_connection_name(name="new_connection_with_new_config")

        self.snowflake.assert_not_called()
        conn.handle
        self.snowflake.assert_has_calls(
            [
                mock.call(
                    account="test-account",
                    autocommit=True,
                    client_session_keep_alive=False,
                    database="test_database",
                    password="test_password",
                    role=None,
                    schema="public",
                    user="test_user",
                    warehouse="test_warehouse",
                    authenticator="test_sso_url",
                    private_key=None,
                    application="dbt",
                    client_request_mfa_token=True,
                    client_store_temporary_credential=True,
                    insecure_mode=False,
                    session_parameters={},
                    reuse_connections=True,
                )
            ]
        )

    def test_authenticator_externalbrowser_authentication(self):
        self.config.credentials = self.config.credentials.replace(authenticator="externalbrowser")
        self.adapter = SnowflakeAdapter(self.config, get_context("spawn"))
        conn = self.adapter.connections.set_connection_name(name="new_connection_with_new_config")

        self.snowflake.assert_not_called()
        conn.handle
        self.snowflake.assert_has_calls(
            [
                mock.call(
                    account="test-account",
                    autocommit=True,
                    client_session_keep_alive=False,
                    database="test_database",
                    role=None,
                    schema="public",
                    user="test_user",
                    warehouse="test_warehouse",
                    authenticator="externalbrowser",
                    private_key=None,
                    application="dbt",
                    client_request_mfa_token=True,
                    client_store_temporary_credential=True,
                    insecure_mode=False,
                    session_parameters={},
                    reuse_connections=True,
                )
            ]
        )

    def test_authenticator_oauth_authentication(self):
        self.config.credentials = self.config.credentials.replace(
            authenticator="oauth",
            token="my-oauth-token",
        )
        self.adapter = SnowflakeAdapter(self.config, get_context("spawn"))
        conn = self.adapter.connections.set_connection_name(name="new_connection_with_new_config")

        self.snowflake.assert_not_called()
        conn.handle
        self.snowflake.assert_has_calls(
            [
                mock.call(
                    account="test-account",
                    autocommit=True,
                    client_session_keep_alive=False,
                    database="test_database",
                    role=None,
                    schema="public",
                    user="test_user",
                    warehouse="test_warehouse",
                    authenticator="oauth",
                    token="my-oauth-token",
                    private_key=None,
                    application="dbt",
                    client_request_mfa_token=True,
                    client_store_temporary_credential=True,
                    insecure_mode=False,
                    session_parameters={},
                    reuse_connections=True,
                )
            ]
        )

    @mock.patch(
        "dbt.adapters.snowflake.SnowflakeCredentials._get_private_key", return_value="test_key"
    )
    def test_authenticator_private_key_authentication(self, mock_get_private_key):
        self.config.credentials = self.config.credentials.replace(
            private_key_path="/tmp/test_key.p8",
            private_key_passphrase="p@ssphr@se",
        )

        self.adapter = SnowflakeAdapter(self.config, get_context("spawn"))
        conn = self.adapter.connections.set_connection_name(name="new_connection_with_new_config")

        self.snowflake.assert_not_called()
        conn.handle
        self.snowflake.assert_has_calls(
            [
                mock.call(
                    account="test-account",
                    autocommit=True,
                    client_session_keep_alive=False,
                    database="test_database",
                    role=None,
                    schema="public",
                    user="test_user",
                    warehouse="test_warehouse",
                    private_key="test_key",
                    application="dbt",
                    insecure_mode=False,
                    session_parameters={},
                    reuse_connections=True,
                )
            ]
        )

    @mock.patch(
        "dbt.adapters.snowflake.SnowflakeCredentials._get_private_key", return_value="test_key"
    )
    def test_authenticator_private_key_authentication_no_passphrase(self, mock_get_private_key):
        self.config.credentials = self.config.credentials.replace(
            private_key_path="/tmp/test_key.p8",
            private_key_passphrase=None,
        )

        self.adapter = SnowflakeAdapter(self.config, get_context("spawn"))
        conn = self.adapter.connections.set_connection_name(name="new_connection_with_new_config")

        self.snowflake.assert_not_called()
        conn.handle
        self.snowflake.assert_has_calls(
            [
                mock.call(
                    account="test-account",
                    autocommit=True,
                    client_session_keep_alive=False,
                    database="test_database",
                    role=None,
                    schema="public",
                    user="test_user",
                    warehouse="test_warehouse",
                    private_key="test_key",
                    application="dbt",
                    insecure_mode=False,
                    session_parameters={},
                    reuse_connections=True,
                )
            ]
        )

    def test_authenticator_jwt_authentication(self):
        self.config.credentials = self.config.credentials.replace(
            authenticator="jwt", token="my-jwt-token", user=None
        )
        self.adapter = SnowflakeAdapter(self.config, get_context("spawn"))
        conn = self.adapter.connections.set_connection_name(name="new_connection_with_new_config")

        self.snowflake.assert_not_called()
        conn.handle
        self.snowflake.assert_has_calls(
            [
                mock.call(
                    account="test-account",
                    autocommit=True,
                    client_session_keep_alive=False,
                    database="test_database",
                    role=None,
                    schema="public",
                    warehouse="test_warehouse",
                    authenticator="oauth",
                    token="my-jwt-token",
                    private_key=None,
                    application="dbt",
                    client_request_mfa_token=True,
                    client_store_temporary_credential=True,
                    insecure_mode=False,
                    session_parameters={},
                    reuse_connections=True,
                )
            ]
        )

    def test_query_tag(self):
        self.config.credentials = self.config.credentials.replace(
            password="test_password", query_tag="test_query_tag"
        )
        self.adapter = SnowflakeAdapter(self.config, get_context("spawn"))
        conn = self.adapter.connections.set_connection_name(name="new_connection_with_new_config")

        self.snowflake.assert_not_called()
        conn.handle
        self.snowflake.assert_has_calls(
            [
                mock.call(
                    account="test-account",
                    autocommit=True,
                    client_session_keep_alive=False,
                    database="test_database",
                    password="test_password",
                    role=None,
                    schema="public",
                    user="test_user",
                    warehouse="test_warehouse",
                    private_key=None,
                    application="dbt",
                    insecure_mode=False,
                    session_parameters={"QUERY_TAG": "test_query_tag"},
                    reuse_connections=True,
                )
            ]
        )

    def test_reuse_connections_with_keep_alive(self):
        self.config.credentials = self.config.credentials.replace(
            reuse_connections=True, client_session_keep_alive=True
        )
        self.adapter = SnowflakeAdapter(self.config, get_context("spawn"))
        conn = self.adapter.connections.set_connection_name(name="new_connection_with_new_config")

        self.snowflake.assert_not_called()
        conn.handle
        self.snowflake.assert_has_calls(
            [
                mock.call(
                    account="test-account",
                    autocommit=True,
                    client_session_keep_alive=True,
                    database="test_database",
                    role=None,
                    schema="public",
                    user="test_user",
                    warehouse="test_warehouse",
                    private_key=None,
                    application="dbt",
                    insecure_mode=False,
                    session_parameters={},
                    reuse_connections=True,
                )
            ]
        )

    @mock.patch(
        "dbt.adapters.snowflake.SnowflakeCredentials._get_private_key", return_value="test_key"
    )
    def test_authenticator_private_key_string_authentication(self, mock_get_private_key):
        self.config.credentials = self.config.credentials.replace(
            private_key="dGVzdF9rZXk=",
            private_key_passphrase="p@ssphr@se",
        )

        self.adapter = SnowflakeAdapter(self.config, get_context("spawn"))
        conn = self.adapter.connections.set_connection_name(name="new_connection_with_new_config")

        self.snowflake.assert_not_called()
        conn.handle
        self.snowflake.assert_has_calls(
            [
                mock.call(
                    account="test-account",
                    autocommit=True,
                    client_session_keep_alive=False,
                    database="test_database",
                    role=None,
                    schema="public",
                    user="test_user",
                    warehouse="test_warehouse",
                    private_key="test_key",
                    application="dbt",
                    insecure_mode=False,
                    session_parameters={},
                    reuse_connections=True,
                )
            ]
        )

    @mock.patch(
        "dbt.adapters.snowflake.SnowflakeCredentials._get_private_key", return_value="test_key"
    )
    def test_authenticator_private_key_string_authentication_no_passphrase(
        self, mock_get_private_key
    ):
        self.config.credentials = self.config.credentials.replace(
            private_key="dGVzdF9rZXk=",
            private_key_passphrase=None,
        )

        self.adapter = SnowflakeAdapter(self.config, get_context("spawn"))
        conn = self.adapter.connections.set_connection_name(name="new_connection_with_new_config")

        self.snowflake.assert_not_called()
        conn.handle
        self.snowflake.assert_has_calls(
            [
                mock.call(
                    account="test-account",
                    autocommit=True,
                    client_session_keep_alive=False,
                    database="test_database",
                    role=None,
                    schema="public",
                    user="test_user",
                    warehouse="test_warehouse",
                    private_key="test_key",
                    application="dbt",
                    insecure_mode=False,
                    session_parameters={},
                    reuse_connections=True,
                )
            ]
        )


class TestSnowflakeAdapterConversions(TestAdapterConversions):
    def test_convert_text_type(self):
        rows = [
            ["", "a1", "stringval1"],
            ["", "a2", "stringvalasdfasdfasdfa"],
            ["", "a3", "stringval3"],
        ]
        agate_table = self._make_table_of(rows, agate.Text)
        expected = ["text", "text", "text"]
        for col_idx, expect in enumerate(expected):
            assert SnowflakeAdapter.convert_text_type(agate_table, col_idx) == expect

    def test_convert_number_type(self):
        rows = [
            ["", "23.98", "-1"],
            ["", "12.78", "-2"],
            ["", "79.41", "-3"],
        ]
        agate_table = self._make_table_of(rows, agate.Number)
        expected = ["integer", "float8", "integer"]
        for col_idx, expect in enumerate(expected):
            assert SnowflakeAdapter.convert_number_type(agate_table, col_idx) == expect

    def test_convert_boolean_type(self):
        rows = [
            ["", "false", "true"],
            ["", "false", "false"],
            ["", "false", "true"],
        ]
        agate_table = self._make_table_of(rows, agate.Boolean)
        expected = ["boolean", "boolean", "boolean"]
        for col_idx, expect in enumerate(expected):
            assert SnowflakeAdapter.convert_boolean_type(agate_table, col_idx) == expect

    def test_convert_datetime_type(self):
        rows = [
            ["", "20190101T01:01:01Z", "2019-01-01 01:01:01"],
            ["", "20190102T01:01:01Z", "2019-01-01 01:01:01"],
            ["", "20190103T01:01:01Z", "2019-01-01 01:01:01"],
        ]
        agate_table = self._make_table_of(
            rows, [agate.DateTime, agate_helper.ISODateTime, agate.DateTime]
        )
        expected = [
            "timestamp without time zone",
            "timestamp without time zone",
            "timestamp without time zone",
        ]
        for col_idx, expect in enumerate(expected):
            assert SnowflakeAdapter.convert_datetime_type(agate_table, col_idx) == expect

    def test_convert_date_type(self):
        rows = [
            ["", "2019-01-01", "2019-01-04"],
            ["", "2019-01-02", "2019-01-04"],
            ["", "2019-01-03", "2019-01-04"],
        ]
        agate_table = self._make_table_of(rows, agate.Date)
        expected = ["date", "date", "date"]
        for col_idx, expect in enumerate(expected):
            assert SnowflakeAdapter.convert_date_type(agate_table, col_idx) == expect

    def test_convert_time_type(self):
        # dbt's default type testers actually don't have a TimeDelta at all.
        agate.TimeDelta
        rows = [
            ["", "120s", "10s"],
            ["", "3m", "11s"],
            ["", "1h", "12s"],
        ]
        agate_table = self._make_table_of(rows, agate.TimeDelta)
        expected = ["time", "time", "time"]
        for col_idx, expect in enumerate(expected):
            assert SnowflakeAdapter.convert_time_type(agate_table, col_idx) == expect


class TestSnowflakeColumn(unittest.TestCase):
    def test_text_from_description(self):
        col = SnowflakeColumn.from_description("my_col", "TEXT")
        assert col.column == "my_col"
        assert col.dtype == "TEXT"
        assert col.char_size is None
        assert col.numeric_precision is None
        assert col.numeric_scale is None
        assert col.is_float() is False
        assert col.is_number() is False
        assert col.is_numeric() is False
        assert col.is_string() is True
        assert col.is_integer() is False
        assert col.string_size() == 16777216

        col = SnowflakeColumn.from_description("my_col", "VARCHAR")
        assert col.column == "my_col"
        assert col.dtype == "VARCHAR"
        assert col.char_size is None
        assert col.numeric_precision is None
        assert col.numeric_scale is None
        assert col.is_float() is False
        assert col.is_number() is False
        assert col.is_numeric() is False
        assert col.is_string() is True
        assert col.is_integer() is False
        assert col.string_size() == 16777216

    def test_sized_varchar_from_description(self):
        col = SnowflakeColumn.from_description("my_col", "VARCHAR(256)")
        assert col.column == "my_col"
        assert col.dtype == "VARCHAR"
        assert col.char_size == 256
        assert col.numeric_precision is None
        assert col.numeric_scale is None
        assert col.is_float() is False
        assert col.is_number() is False
        assert col.is_numeric() is False
        assert col.is_string() is True
        assert col.is_integer() is False
        assert col.string_size() == 256

    def test_sized_decimal_from_description(self):
        col = SnowflakeColumn.from_description("my_col", "DECIMAL(1, 0)")
        assert col.column == "my_col"
        assert col.dtype == "DECIMAL"
        assert col.char_size is None
        assert col.numeric_precision == 1
        assert col.numeric_scale == 0
        assert col.is_float() is False
        assert col.is_number() is True
        assert col.is_numeric() is True
        assert col.is_string() is False
        assert col.is_integer() is False

    def test_float_from_description(self):
        col = SnowflakeColumn.from_description("my_col", "FLOAT8")
        assert col.column == "my_col"
        assert col.dtype == "FLOAT8"
        assert col.char_size is None
        assert col.numeric_precision is None
        assert col.numeric_scale is None
        assert col.is_float() is True
        assert col.is_number() is True
        assert col.is_numeric() is False
        assert col.is_string() is False
        assert col.is_integer() is False

    def test_vector_from_description(self):
        col = SnowflakeColumn.from_description("my_col", "VECTOR(FLOAT, 768)")
        assert col.column == "my_col"
        assert col.dtype == "VECTOR(FLOAT, 768)"
        assert col.char_size is None
        assert col.numeric_precision is None
        assert col.numeric_scale is None
        assert col.is_float() is False
        assert col.is_number() is False
        assert col.is_numeric() is False
        assert col.is_string() is False
        assert col.is_integer() is False


class SnowflakeConnectionsTest(unittest.TestCase):
    def test_comment_stripping_regex(self):
        pattern = r"(\".*?\"|\'.*?\')|(/\*.*?\*/|--[^\r\n]*$)"
        comment1 = "-- just comment"
        comment2 = "/* just comment */"
        query1 = "select 1; -- comment"
        query2 = "select 1; /* comment */"
        query3 = "select 1; -- comment\nselect 2; /* comment */ "
        query4 = "select \n1; -- comment\nselect \n2; /* comment */ "
        query5 = "select 1; -- comment \nselect 2; -- comment \nselect 3; -- comment"

        stripped_comment1 = re.sub(re.compile(pattern, re.MULTILINE), "", comment1).strip()

        stripped_comment2 = re.sub(re.compile(pattern, re.MULTILINE), "", comment2).strip()

        stripped_query1 = re.sub(re.compile(pattern, re.MULTILINE), "", query1).strip()

        stripped_query2 = re.sub(re.compile(pattern, re.MULTILINE), "", query2).strip()

        stripped_query3 = re.sub(re.compile(pattern, re.MULTILINE), "", query3).strip()

        stripped_query4 = re.sub(re.compile(pattern, re.MULTILINE), "", query4).strip()

        stripped_query5 = re.sub(re.compile(pattern, re.MULTILINE), "", query5).strip()

        expected_query_3 = "select 1; \nselect 2;"
        expected_query_4 = "select \n1; \nselect \n2;"
        expected_query_5 = "select 1; \nselect 2; \nselect 3;"

        self.assertEqual("", stripped_comment1)
        self.assertEqual("", stripped_comment2)
        self.assertEqual("select 1;", stripped_query1)
        self.assertEqual("select 1;", stripped_query2)
        self.assertEqual(expected_query_3, stripped_query3)
        self.assertEqual(expected_query_4, stripped_query4)
        self.assertEqual(expected_query_5, stripped_query5)


class TestSnowflakeAdapterCredentials(unittest.TestCase):
    unencrypted_private_key = (
        b"0\x82\x01T\x02\x01\x000\r\x06\t*\x86H\x86\xf7\r\x01\x01\x01"
        b"\x05\x00\x04\x82\x01>0\x82\x01:\x02\x01\x00\x02A\x00\xd9."
        b"\x15\xc8\xce\xfa\x1c\x9a\xe8/|5lf\xb8\xd9\x13\xc5I\x16i \x9f"
        b"'rO\xb1RkD(\n\xff\x84v\xbaS\x8d\xb46\xf8\x85w\x81\xe2\xc5cy"
        b"\xf1\xb6\xa9i]F\xfc\x04e`\xfbw;\x91\xf5\xcf\x02\x03\x01\x00"
        b"\x01\x02A\x00\x81\x84\xc6a\x17ny\x98\xb8WyO\xb2\xf2\x1f\xd2"
        b"\xf5\xc3v.\xf3K\r\x1fM@\xd1\x93A}H\x13\r\xa7\xd4\n,7L\x14?"
        b"\xff\xe2\xf3\xac\x93\xbb\xdf\xc3\xe5\xea\xf1AG\xc0~\xa2\x9a6"
        b"6\xeb\x11S\xe1\x02!\x00\xf3\x1d\xf1\xcc\xecj\xaf}\x01\xd4"
        b"\xee\x84\x03(Qx9\x9f\xedH\xf1\x016r\xbaE{Uk\x9d,\x13\x02!"
        b"\x00\xe4\xb0I\x8b\x8bU\xe5\x9a\x93V\x9f\xa8Ui\x9cQ\xd7\x12"
        b"\x866g<MK\xa4J5\xb0\xc6\xf7\xce\xd5\x02 wp=\xab\xe4v!R\xf3"
        b"\xc4m\x8d\x93\x93\x8a:\xdbl\x93\x81\xa3Mj7\x81\x05\xc3\xaa"
        b"\xda\x9c\xb3\xdb\x02 w\xafW^C\xd6\xf9\xaasp\x03p\xfa\xfa\xa1"
        b"\xc8'2W\xb1\x83H\t\x00\x0c\x84\x96\"\xe5\x8e\xed\xdd\x02 "
        b'\x1d\\\x02\xbc\xf3\xff\x17":>\xdf[{y\xe8\xc1j\xda\xc2\xa9'
        b"\x9d\x94)_\xe6\x9c\xf1FF\x03Y\x81"
    )
    unencrypted_private_key_encoded = (
        "MIIBVAIBADANBgkqhkiG9w0BAQEFAASCAT4wggE6AgEAAkEA2S4VyM76HJroL3w1bGa42"
        "RPFSRZpIJ8nck+xUmtEKAr/hHa6U420NviFd4HixWN58bapaV1G/ARlYPt3O5H1zwIDAQ"
        "ABAkEAgYTGYRdueZi4V3lPsvIf0vXDdi7zSw0fTUDRk0F9SBMNp9QKLDdMFD//4vOsk7v"
        "fw+Xq8UFHwH6imjY26xFT4QIhAPMd8czsaq99AdTuhAMoUXg5n+1I8QE2crpFe1VrnSwT"
        "AiEA5LBJi4tV5ZqTVp+oVWmcUdcShjZnPE1LpEo1sMb3ztUCIHdwPavkdiFS88RtjZOTi"
        "jrbbJOBo01qN4EFw6ranLPbAiB3r1deQ9b5qnNwA3D6+qHIJzJXsYNICQAMhJYi5Y7t3Q"
        "IgHVwCvPP/FyI6Pt9be3nowWrawqmdlClf5pzxRkYDWYE="
    )
    encrypted_private_key_encoded = (
        "MIIBvTBXBgkqhkiG9w0BBQ0wSjApBgkqhkiG9w0BBQwwHAQIIxdFbtlFbgkCAggAMAwGC"
        "CqGSIb3DQIJBQAwHQYJYIZIAWUDBAEqBBBswzZouyovzUADDQcLUEwgBIIBYAiB+rnGhm"
        "PwKZYOFdyEvkXFFu2aRfqotYHy/qlfVdU4BfNHwBlAlgUOPMN2HJ9KwyiNdBKoQ1Z4KXI"
        "G4AU74QZsVSL+miFf65qqWKLwckL45Y3WMUH1K0YpdO0W+aznjH9msWYuM/zCpQS2rvVX"
        "a5rXpA5praB5nn6kRlTwrQ8DN0ZKOKBX6ojhSE/6TmQtx3d+tmly8ZpTkG5HVTuBMCtDg"
        "Po6mAEvvb4T/dnx9MtUz0d5AgNuVOS5+OI32kX9XVupEGkvdY8iHbx7+skbKFVdHMayBL"
        "0dy5knySv+YGi/T6oM0uApPPk4aT493MNzT8544Wmi/NbNkDx+6XQiTuPZ4OsL0iF3KsX"
        "xfTC4tDGGYn4yW8bnSz3K+lkXA9vyie37nW6ncu+aizT9TgD1q6jFm9u/1G61/Z96oHgz"
        "pVghkP6s3l23U/7qM2PC8CEu18nUDYEhrv6lOwr8EABHV0s="
    )
    encrypted_private_key_passphrase = "insecure"

    def test_private_key_string(self):
        creds = SnowflakeCredentials(
            account="test-account",
            user="test_user",
            database="test_database",
            schema="public",
            private_key=self.unencrypted_private_key_encoded,
        )
        self.assertEqual(creds.auth_args()["private_key"], self.unencrypted_private_key)

    def test_private_key_string_encrypted(self):
        creds = SnowflakeCredentials(
            account="test-account",
            user="test_user",
            database="test_database",
            schema="public",
            private_key=self.encrypted_private_key_encoded,
            private_key_passphrase=self.encrypted_private_key_passphrase,
        )
        self.assertEqual(creds.auth_args()["private_key"], self.unencrypted_private_key)

    def test_malformed_private_key_string(self):
        creds = SnowflakeCredentials(
            account="test-account",
            user="test_user",
            database="test_database",
            schema="public",
            private_key="dGVzdF9rZXk=",
        )
        self.assertRaises(ValueError, creds.auth_args)

    def test_invalid_private_key_string(self):
        creds = SnowflakeCredentials(
            account="test-account",
            user="test_user",
            database="test_database",
            schema="public",
            private_key="invalid[base64]=",
        )
        self.assertRaises(ValueError, creds.auth_args)

    def test_invalid_private_key_path(self):
        creds = SnowflakeCredentials(
            account="test-account",
            user="test_user",
            database="test_database",
            schema="public",
            private_key_path="/tmp/does/not/exist.p8",
        )
        self.assertRaises(FileNotFoundError, creds.auth_args)
