import os
from importlib import reload
from unittest.mock import Mock
import dbt.adapters.snowflake.connections as connections
import dbt.adapters.events.logging


def test_connections_sets_logs_in_response_to_env_var(monkeypatch):
    """Test that setting the DBT_SNOWFLAKE_CONNECTOR_DEBUG_LOGGING environment variable happens on import"""
    log_mock = Mock()
    monkeypatch.setattr(dbt.adapters.events.logging, "AdapterLogger", Mock(return_value=log_mock))
    monkeypatch.setattr(os, "environ", {"DBT_SNOWFLAKE_CONNECTOR_DEBUG_LOGGING": "true"})
    reload(connections)

    assert log_mock.debug.call_count == 3
    assert log_mock.set_adapter_dependency_log_level.call_count == 3


def test_connections_does_not_set_logs_in_response_to_env_var(monkeypatch):
    log_mock = Mock()
    monkeypatch.setattr(dbt.adapters.events.logging, "AdapterLogger", Mock(return_value=log_mock))
    reload(connections)

    assert log_mock.debug.call_count == 0
    assert log_mock.set_adapter_dependency_log_level.call_count == 0
