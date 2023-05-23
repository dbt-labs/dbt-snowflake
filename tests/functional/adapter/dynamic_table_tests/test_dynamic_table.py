import pytest

from dbt.contracts.results import RunStatus
from dbt.adapters.snowflake.relation import SnowflakeRelationType
from dbt.tests.adapter.materialized_view.base import (
    run_model,
    assert_model_exists_and_is_correct_type,
    insert_record,
    get_row_count,
)
from dbt.tests.adapter.materialized_view.on_configuration_change import assert_proper_scenario

from tests.functional.adapter.dynamic_table_tests.fixtures import (
    SnowflakeBasicBase,
    SnowflakeOnConfigurationChangeBase,
)


class TestBasic(SnowflakeBasicBase):
    def test_relation_is_dynamic_table_on_initial_creation(self, project):
        assert_model_exists_and_is_correct_type(
            project, "base_dynamic_table", SnowflakeRelationType.DynamicTable
        )
        assert_model_exists_and_is_correct_type(project, "base_table", SnowflakeRelationType.Table)

    def test_relation_is_dynamic_table_when_rerun(self, project):
        run_model("base_dynamic_table")
        assert_model_exists_and_is_correct_type(
            project, "base_dynamic_table", SnowflakeRelationType.DynamicTable
        )

    def test_relation_is_dynamic_table_on_full_refresh(self, project):
        run_model("base_dynamic_table", full_refresh=True)
        assert_model_exists_and_is_correct_type(
            project, "base_dynamic_table", SnowflakeRelationType.DynamicTable
        )

    def test_relation_is_dynamic_table_on_update(self, project):
        run_model("base_dynamic_table", run_args=["--vars", "quoting: {identifier: True}"])
        assert_model_exists_and_is_correct_type(
            project, "base_dynamic_table", SnowflakeRelationType.DynamicTable
        )

    @pytest.mark.skip("Fails because stub uses traditional view")
    def test_updated_base_table_data_only_shows_in_dynamic_table_after_rerun(self, project):
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


class OnConfigurationChangeCommon(SnowflakeOnConfigurationChangeBase):
    def test_full_refresh_takes_precedence_over_any_configuration_changes(
        self, configuration_changes, replace_message, configuration_change_message
    ):
        results, logs = run_model("base_dynamic_table", full_refresh=True)
        assert_proper_scenario(
            self.on_configuration_change,
            results,
            logs,
            RunStatus.Success,
            messages_in_logs=[replace_message],
            messages_not_in_logs=[configuration_change_message],
        )

    def test_model_is_refreshed_with_no_configuration_changes(
        self, refresh_message, configuration_change_message
    ):
        results, logs = run_model("base_dynamic_table")
        assert_proper_scenario(
            self.on_configuration_change,
            results,
            logs,
            RunStatus.Success,
            messages_in_logs=[refresh_message, configuration_change_message],
        )


class TestOnConfigurationChangeApply(OnConfigurationChangeCommon):
    @pytest.mark.skip("This fails because there are no changes in the stub")
    def test_model_applies_changes_with_configuration_changes(
        self, configuration_changes, alter_message, update_index_message
    ):
        results, logs = run_model("base_dynamic_table")
        assert_proper_scenario(
            self.on_configuration_change,
            results,
            logs,
            RunStatus.Success,
            messages_in_logs=[alter_message, update_index_message],
        )


class TestOnConfigurationChangeSkip(OnConfigurationChangeCommon):
    on_configuration_change = "skip"

    @pytest.mark.skip("This fails because there are no changes in the stub")
    def test_model_is_skipped_with_configuration_changes(
        self, configuration_changes, configuration_change_skip_message
    ):
        results, logs = run_model("base_dynamic_table")
        assert_proper_scenario(
            self.on_configuration_change,
            results,
            logs,
            RunStatus.Success,
            messages_in_logs=[configuration_change_skip_message],
        )


class TestOnConfigurationChangeFail(OnConfigurationChangeCommon):
    on_configuration_change = "fail"

    @pytest.mark.skip("This fails because there are no changes in the stub")
    def test_run_fails_with_configuration_changes(
        self, configuration_changes, configuration_change_fail_message
    ):
        results, logs = run_model("base_dynamic_table", expect_pass=False)
        assert_proper_scenario(
            self.on_configuration_change,
            results,
            logs,
            RunStatus.Error,
            messages_in_logs=[configuration_change_fail_message],
        )
