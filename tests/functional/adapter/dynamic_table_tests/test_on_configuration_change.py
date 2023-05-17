import pytest

from dbt.contracts.results import RunStatus
from dbt.tests.util import relation_from_name
from dbt.tests.adapter.materialized_view.base import run_model
from dbt.tests.adapter.materialized_view.on_configuration_change import OnConfigurationChangeBase

from tests.functional.adapter.dynamic_table_tests.test_basic import SnowflakeBase


class SnowflakeOnConfigurationChangeBase(SnowflakeBase, OnConfigurationChangeBase):
    @pytest.fixture(scope="function")
    def configuration_changes(self, project):
        pass

    @pytest.fixture(scope="function")
    def configuration_change_message(self, project):
        # We need to do this because the default quote policy is overriden
        # in `SnowflakeAdapter.list_relations_without_caching`; we wind up with
        # an uppercase quoted name when supplied with a lowercase name with un-quoted quote policy.
        relation = relation_from_name(project.adapter, self.base_materialized_view.name)
        database, schema, name = str(relation).split(".")
        relation_upper = f'"{database.upper()}"."{schema.upper()}"."{name.upper()}"'
        return f"Determining configuration changes on: {relation_upper}"

    def test_full_refresh_takes_precedence_over_any_configuration_changes(
        self, configuration_changes, replace_message, configuration_change_message
    ):
        results, logs = run_model(self.base_materialized_view.name, full_refresh=True)
        self.assert_proper_scenario(
            results,
            logs,
            RunStatus.Success,
            messages_in_logs=[replace_message],
            messages_not_in_logs=[configuration_change_message],
        )

    def test_model_is_refreshed_with_no_configuration_changes(
        self, refresh_message, configuration_change_message
    ):
        results, logs = run_model(self.base_materialized_view.name)
        self.assert_proper_scenario(
            results,
            logs,
            RunStatus.Success,
            messages_in_logs=[refresh_message, configuration_change_message],
        )


class TestOnConfigurationChangeApply(SnowflakeOnConfigurationChangeBase):
    on_configuration_change = "apply"

    @pytest.mark.skip("This fails because there are no changes in the stub")
    def test_model_applies_changes_with_configuration_changes(
        self, configuration_changes, alter_message, update_index_message
    ):
        results, logs = run_model(self.base_dynamic_table.name)
        self.assert_proper_scenario(
            results,
            logs,
            RunStatus.Success,
            messages_in_logs=[alter_message, update_index_message],
        )


class TestOnConfigurationChangeSkip(SnowflakeOnConfigurationChangeBase):
    on_configuration_change = "skip"

    @pytest.mark.skip("This fails because there are no changes in the stub")
    def test_model_is_skipped_with_configuration_changes(
        self, configuration_changes, configuration_change_skip_message
    ):
        results, logs = run_model(self.base_dynamic_table.name)
        self.assert_proper_scenario(
            results, logs, RunStatus.Success, messages_in_logs=[configuration_change_skip_message]
        )


class TestOnConfigurationChangeFail(SnowflakeOnConfigurationChangeBase):
    on_configuration_change = "fail"

    @pytest.mark.skip("This fails because there are no changes in the stub")
    def test_run_fails_with_configuration_changes(
        self, configuration_changes, configuration_change_fail_message
    ):
        results, logs = run_model(self.base_dynamic_table.name, expect_pass=False)
        self.assert_proper_scenario(
            results, logs, RunStatus.Error, messages_in_logs=[configuration_change_fail_message]
        )
