import pytest

from dbt.contracts.results import RunStatus
from dbt.contracts.graph.model_config import OnConfigurationChangeOption
from dbt.tests.adapter.materialized_view.base import (
    run_model,
    insert_record,
)
from dbt.tests.adapter.materialized_view.on_configuration_change import assert_proper_scenario

from dbt.adapters.snowflake.relation import SnowflakeRelationType
from tests.functional.adapter.dynamic_table_tests.fixtures import (
    SnowflakeBasicBase,
    SnowflakeOnConfigurationChangeBase,
    assert_model_exists_and_is_correct_type,
    refresh_dynamic_table,
    get_row_count,
)


class TestBasic(SnowflakeBasicBase):
    def test_relation_is_dynamic_table_on_initial_creation(self, project, adapter):
        refresh_dynamic_table(adapter, "base_dynamic_table")
        assert_model_exists_and_is_correct_type(
            project, "base_dynamic_table", SnowflakeRelationType.DynamicTable
        )
        assert_model_exists_and_is_correct_type(project, "base_table", SnowflakeRelationType.Table)

    def test_relation_is_dynamic_table_when_rerun(self, project, adapter):
        run_model("base_dynamic_table")
        refresh_dynamic_table(adapter, "base_dynamic_table")
        assert_model_exists_and_is_correct_type(
            project, "base_dynamic_table", SnowflakeRelationType.DynamicTable
        )

    def test_relation_is_dynamic_table_on_full_refresh(self, project, adapter):
        run_model("base_dynamic_table", full_refresh=True)
        refresh_dynamic_table(adapter, "base_dynamic_table")
        assert_model_exists_and_is_correct_type(
            project, "base_dynamic_table", SnowflakeRelationType.DynamicTable
        )

    def test_relation_is_dynamic_table_on_update(self, project, adapter):
        run_model("base_dynamic_table", run_args=["--vars", "quoting: {identifier: True}"])
        refresh_dynamic_table(adapter, "base_dynamic_table")
        assert_model_exists_and_is_correct_type(
            project, "base_dynamic_table", SnowflakeRelationType.DynamicTable
        )

    def test_updated_base_table_data_only_shows_in_dynamic_table_after_rerun(
        self, project, adapter
    ):
        refresh_dynamic_table(adapter, "base_dynamic_table")

        # poll database
        table_start = get_row_count(project, "base_table")
        dyn_start = get_row_count(project, "base_dynamic_table")

        # insert new record in table
        new_record = (2,)
        insert_record(project, new_record, "base_table", ["base_column"])

        # poll database
        table_mid = get_row_count(project, "base_table")
        dyn_mid = get_row_count(project, "base_dynamic_table")

        # refresh the dynamic table
        run_model("base_dynamic_table")

        # poll database
        table_end = get_row_count(project, "base_table")
        dyn_end = get_row_count(project, "base_dynamic_table")

        # new records were inserted in the table but didn't show up in the dynamic table until it was refreshed
        assert table_start < table_mid == table_end
        assert dyn_start == dyn_mid < dyn_end


@pytest.mark.skip("We're not looking for changes yet")
class TestOnConfigurationChangeApply(SnowflakeOnConfigurationChangeBase):
    # we don't need to specify OnConfigurationChangeOption.Apply because it's the default
    # this is part of the test

    def test_full_refresh_takes_precedence_over_any_configuration_changes(
        self, configuration_changes, replace_message, configuration_change_message, adapter
    ):
        refresh_dynamic_table(adapter, "base_dynamic_table")
        results, logs = run_model("base_dynamic_table", full_refresh=True)
        assert_proper_scenario(
            OnConfigurationChangeOption.Apply,
            results,
            logs,
            RunStatus.Success,
            messages_in_logs=[replace_message],
            messages_not_in_logs=[configuration_change_message],
        )

    def test_model_is_refreshed_with_no_configuration_changes(
        self, refresh_message, configuration_change_message, adapter
    ):
        refresh_dynamic_table(adapter, "base_dynamic_table")
        results, logs = run_model("base_dynamic_table")
        assert_proper_scenario(
            OnConfigurationChangeOption.Apply,
            results,
            logs,
            RunStatus.Success,
            messages_in_logs=[refresh_message, configuration_change_message],
        )

    def test_model_applies_changes_with_configuration_changes(
        self, configuration_changes, alter_message, update_index_message, adapter
    ):
        refresh_dynamic_table(adapter, "base_dynamic_table")
        results, logs = run_model("base_dynamic_table")
        assert_proper_scenario(
            OnConfigurationChangeOption.Apply,
            results,
            logs,
            RunStatus.Success,
            messages_in_logs=[alter_message, update_index_message],
        )


@pytest.mark.skip("We're not looking for changes yet")
class TestOnConfigurationChangeContinue(SnowflakeOnConfigurationChangeBase):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"on_configuration_change": OnConfigurationChangeOption.Continue.value}}

    def test_full_refresh_takes_precedence_over_any_configuration_changes(
        self, configuration_changes, replace_message, configuration_change_message, adapter
    ):
        refresh_dynamic_table(adapter, "base_dynamic_table")
        results, logs = run_model("base_dynamic_table", full_refresh=True)
        assert_proper_scenario(
            OnConfigurationChangeOption.Continue,
            results,
            logs,
            RunStatus.Success,
            messages_in_logs=[replace_message],
            messages_not_in_logs=[configuration_change_message],
        )

    def test_model_is_refreshed_with_no_configuration_changes(
        self, refresh_message, configuration_change_message, adapter
    ):
        refresh_dynamic_table(adapter, "base_dynamic_table")
        results, logs = run_model("base_dynamic_table")
        assert_proper_scenario(
            OnConfigurationChangeOption.Continue,
            results,
            logs,
            RunStatus.Success,
            messages_in_logs=[refresh_message, configuration_change_message],
        )

    def test_model_is_skipped_with_configuration_changes(
        self, configuration_changes, configuration_change_skip_message, adapter
    ):
        refresh_dynamic_table(adapter, "base_dynamic_table")
        results, logs = run_model("base_dynamic_table")
        assert_proper_scenario(
            OnConfigurationChangeOption.Continue,
            results,
            logs,
            RunStatus.Success,
            messages_in_logs=[configuration_change_skip_message],
        )


@pytest.mark.skip("We're not looking for changes yet")
class TestOnConfigurationChangeFail(SnowflakeOnConfigurationChangeBase):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"on_configuration_change": OnConfigurationChangeOption.Fail.value}}

    def test_full_refresh_takes_precedence_over_any_configuration_changes(
        self, configuration_changes, replace_message, configuration_change_message, adapter
    ):
        refresh_dynamic_table(adapter, "base_dynamic_table")
        results, logs = run_model("base_dynamic_table", full_refresh=True)
        assert_proper_scenario(
            OnConfigurationChangeOption.Fail,
            results,
            logs,
            RunStatus.Success,
            messages_in_logs=[replace_message],
            messages_not_in_logs=[configuration_change_message],
        )

    def test_model_is_refreshed_with_no_configuration_changes(
        self, refresh_message, configuration_change_message, adapter
    ):
        refresh_dynamic_table(adapter, "base_dynamic_table")
        results, logs = run_model("base_dynamic_table")
        assert_proper_scenario(
            OnConfigurationChangeOption.Fail,
            results,
            logs,
            RunStatus.Success,
            messages_in_logs=[refresh_message, configuration_change_message],
        )

    def test_run_fails_with_configuration_changes(
        self, configuration_changes, configuration_change_fail_message, adapter
    ):
        refresh_dynamic_table(adapter, "base_dynamic_table")
        results, logs = run_model("base_dynamic_table", expect_pass=False)
        assert_proper_scenario(
            OnConfigurationChangeOption.Fail,
            results,
            logs,
            RunStatus.Error,
            messages_in_logs=[configuration_change_fail_message],
        )
