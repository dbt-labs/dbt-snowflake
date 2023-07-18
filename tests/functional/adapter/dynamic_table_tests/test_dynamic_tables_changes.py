from typing import Optional

import pytest

from dbt.contracts.graph.model_config import OnConfigurationChangeOption
from dbt.tests.util import (
    assert_message_in_logs,
    get_model_file,
    run_dbt,
    run_dbt_and_capture,
    set_model_file,
)

from dbt.adapters.snowflake.relation import SnowflakeRelation, SnowflakeRelationType
from tests.functional.adapter.dynamic_table_tests.files import (
    MY_DYNAMIC_TABLE,
    MY_SEED,
)
from tests.functional.adapter.dynamic_table_tests.utils import (
    query_relation_type,
    query_target_lag,
    query_warehouse,
)


class SnowflakeDynamicTableChanges:
    @staticmethod
    def check_start_state(project, dynamic_table):
        """
        This needs to be done manually for now until we fix the test suite's runner. The test suite's
        runner cannot run queries with multiple statements. Snowflake's metadata is all behind `show`
        and `describe` calls that require a second call to fetch the results; hence, the results
        cannot be fetched.
        """
        assert query_target_lag(project, dynamic_table) is None == "60 seconds"
        assert query_warehouse(project, dynamic_table) is None == "DBT_TESTING"

    @staticmethod
    def change_config_via_alter(project, dynamic_table):
        initial_model = get_model_file(project, dynamic_table)
        new_model = initial_model.replace("target_lag='60 seconds'", "target_lag='5 minutes'")
        set_model_file(project, dynamic_table, new_model)

    @staticmethod
    def check_state_alter_change_is_applied(project, dynamic_table):
        # see above
        assert query_target_lag(project, dynamic_table) == "5 minutes"
        assert query_warehouse(project, dynamic_table) == "DBT_TESTING"

    @staticmethod
    def change_config_via_replace(project, dynamic_table):
        # dbt-snowflake does not currently monitor any changes that trigger a full refresh
        pass

    @staticmethod
    def check_state_replace_change_is_applied(project, dynamic_table):
        # dbt-snowflake does not currently monitor any changes that trigger a full refresh
        pass

    @staticmethod
    def query_relation_type(project, relation: SnowflakeRelation) -> Optional[str]:
        return query_relation_type(project, relation)

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": MY_SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {"my_dynamic_table.sql": MY_DYNAMIC_TABLE}

    @pytest.fixture(scope="class")
    def my_dynamic_table(self, project) -> SnowflakeRelation:
        return project.adapter.Relation.create(
            identifier="my_dynamic_table",
            schema=project.test_schema,
            database=project.database,
            type=SnowflakeRelationType.DynamicTable,
        )

    @pytest.fixture(scope="function", autouse=True)
    def setup(self, project, my_dynamic_table):
        # make sure the model in the data reflects the files each time
        run_dbt(["seed"])
        run_dbt(["run", "--models", my_dynamic_table.identifier, "--full-refresh"])

        # the tests touch these files, store their contents in memory
        initial_model = get_model_file(project, my_dynamic_table)

        yield

        # and then reset them after the test runs
        set_model_file(project, my_dynamic_table, initial_model)

        # ensure clean slate each method
        project.run_sql(f"drop schema if exists {project.test_schema} cascade")

    def test_full_refresh_occurs_with_changes(self, project, my_dynamic_table):
        self.change_config_via_alter(project, my_dynamic_table)
        self.change_config_via_replace(project, my_dynamic_table)
        _, logs = run_dbt_and_capture(
            ["--debug", "run", "--models", my_dynamic_table.identifier, "--full-refresh"]
        )
        assert self.query_relation_type(project, my_dynamic_table) == "dynamic_table"
        assert_message_in_logs(f"Applying ALTER to: {my_dynamic_table}", logs, False)
        assert_message_in_logs(f"Applying REPLACE to: {my_dynamic_table}", logs)


class TestSnowflakeDynamicTableChangesApply(SnowflakeDynamicTableChanges):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"on_configuration_change": OnConfigurationChangeOption.Apply.value}}

    @pytest.mark.skip(
        "all changes are currently resulting in a full refresh, regardless of on_configuration_change"
    )
    def test_change_is_applied_via_alter(self, project, my_dynamic_table):
        """
        See above about the two commented assertions. In the meantime, these have been validated manually.
        """
        # self.check_start_state(project, my_dynamic_table)

        self.change_config_via_alter(project, my_dynamic_table)
        _, logs = run_dbt_and_capture(["--debug", "run", "--models", my_dynamic_table.name])

        # self.check_state_alter_change_is_applied(project, my_dynamic_table)

        assert_message_in_logs(f"Applying ALTER to: {str(my_dynamic_table).upper()}", logs)
        assert_message_in_logs(
            f"Applying REPLACE to: {str(my_dynamic_table).upper()}", logs, False
        )

    @pytest.mark.skip(
        "dbt-snowflake does not currently monitor any changes the trigger a full refresh"
    )
    def test_change_is_applied_via_replace(self, project, my_dynamic_table):
        self.check_start_state(project, my_dynamic_table)

        self.change_config_via_alter(project, my_dynamic_table)
        self.change_config_via_replace(project, my_dynamic_table)
        _, logs = run_dbt_and_capture(["--debug", "run", "--models", my_dynamic_table.name])

        self.check_state_alter_change_is_applied(project, my_dynamic_table)
        self.check_state_replace_change_is_applied(project, my_dynamic_table)

        assert_message_in_logs(f"Applying REPLACE to: {my_dynamic_table}", logs)


class TestSnowflakeDynamicTableChangesContinue(SnowflakeDynamicTableChanges):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"on_configuration_change": OnConfigurationChangeOption.Continue.value}}

    @pytest.mark.skip(
        "all changes are currently resulting in a full refresh, regardless of on_configuration_change"
    )
    def test_change_is_not_applied_via_alter(self, project, my_dynamic_table):
        """
        See above about the two commented assertions. In the meantime, these have been validated manually.
        """
        # self.check_start_state(project, my_dynamic_table)

        self.change_config_via_alter(project, my_dynamic_table)
        _, logs = run_dbt_and_capture(["--debug", "run", "--models", my_dynamic_table.name])

        # self.check_start_state(project, my_dynamic_table)

        assert_message_in_logs(
            f"Configuration changes were identified and `on_configuration_change` was set"
            f" to `continue` for `{my_dynamic_table}`",
            logs,
        )
        assert_message_in_logs(f"Applying ALTER to: {my_dynamic_table}", logs, False)
        assert_message_in_logs(f"Applying REPLACE to: {my_dynamic_table}", logs, False)

    @pytest.mark.skip(
        "dbt-snowflake does not currently monitor any changes the trigger a full refresh"
    )
    def test_change_is_not_applied_via_replace(self, project, my_dynamic_table):
        self.check_start_state(project, my_dynamic_table)

        self.change_config_via_alter(project, my_dynamic_table)
        self.change_config_via_replace(project, my_dynamic_table)
        _, logs = run_dbt_and_capture(["--debug", "run", "--models", my_dynamic_table.name])

        self.check_start_state(project, my_dynamic_table)

        assert_message_in_logs(
            f"Configuration changes were identified and `on_configuration_change` was set"
            f" to `continue` for `{my_dynamic_table}`",
            logs,
        )
        assert_message_in_logs(f"Applying ALTER to: {my_dynamic_table}", logs, False)
        assert_message_in_logs(f"Applying REPLACE to: {my_dynamic_table}", logs, False)


class TestSnowflakeDynamicTableChangesFailMixin(SnowflakeDynamicTableChanges):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"on_configuration_change": OnConfigurationChangeOption.Fail.value}}

    @pytest.mark.skip(
        "all changes are currently resulting in a full refresh, regardless of on_configuration_change"
    )
    def test_change_is_not_applied_via_alter(self, project, my_dynamic_table):
        """
        See above about the two commented assertions. In the meantime, these have been validated manually.
        """
        # self.check_start_state(project, my_dynamic_table)

        self.change_config_via_alter(project, my_dynamic_table)
        _, logs = run_dbt_and_capture(
            ["--debug", "run", "--models", my_dynamic_table.name], expect_pass=False
        )

        # self.check_start_state(project, my_dynamic_table)

        assert_message_in_logs(
            f"Configuration changes were identified and `on_configuration_change` was set"
            f" to `fail` for `{my_dynamic_table}`",
            logs,
        )
        assert_message_in_logs(f"Applying ALTER to: {my_dynamic_table}", logs, False)
        assert_message_in_logs(f"Applying REPLACE to: {my_dynamic_table}", logs, False)

    @pytest.mark.skip(
        "dbt-snowflake does not currently monitor any changes the trigger a full refresh"
    )
    def test_change_is_not_applied_via_replace(self, project, my_dynamic_table):
        self.check_start_state(project, my_dynamic_table)

        self.change_config_via_alter(project, my_dynamic_table)
        self.change_config_via_replace(project, my_dynamic_table)
        _, logs = run_dbt_and_capture(
            ["--debug", "run", "--models", my_dynamic_table.name], expect_pass=False
        )

        self.check_start_state(project, my_dynamic_table)

        assert_message_in_logs(
            f"Configuration changes were identified and `on_configuration_change` was set"
            f" to `fail` for `{my_dynamic_table}`",
            logs,
        )
        assert_message_in_logs(f"Applying ALTER to: {my_dynamic_table}", logs, False)
        assert_message_in_logs(f"Applying REPLACE to: {my_dynamic_table}", logs, False)
