import pytest

from dbt.contracts.graph.model_config import OnConfigurationChangeOption
from dbt.tests.util import (
    assert_message_in_logs,
    get_model_file,
    run_dbt,
    run_dbt_and_capture,
    set_model_file,
)
from tests.functional.adapter.dynamic_table_tests.files import (
    MY_DYNAMIC_TABLE,
    MY_SEED,
    MY_TABLE,
    MY_VIEW,
)
from tests.functional.adapter.dynamic_table_tests.utils import (
    query_relation_type,
    query_row_count,
    # query_target_lag,
    swap_dynamic_table_to_table,
    swap_dynamic_table_to_view,
    swap_target_lag,
)


@pytest.fixture(scope="class", autouse=True)
def seeds():
    return {"my_seed.csv": MY_SEED}


@pytest.fixture(scope="class", autouse=True)
def models():
    yield {
        "my_table.sql": MY_TABLE,
        "my_view.sql": MY_VIEW,
        "my_dynamic_table.sql": MY_DYNAMIC_TABLE,
    }


@pytest.fixture(scope="class", autouse=True)
def setup(project):
    run_dbt(["seed"])
    yield


def test_dynamic_table_create(project, my_dynamic_table):
    assert query_relation_type(project, my_dynamic_table) is None
    run_dbt(["run", "--models", my_dynamic_table.name])
    assert query_relation_type(project, my_dynamic_table) == "dynamic_table"


def test_dynamic_table_create_idempotent(project, my_dynamic_table):
    assert query_relation_type(project, my_dynamic_table) is None
    run_dbt(["run", "--models", my_dynamic_table.name])
    assert query_relation_type(project, my_dynamic_table) == "dynamic_table"
    run_dbt(["run", "--models", my_dynamic_table.name])
    assert query_relation_type(project, my_dynamic_table) == "dynamic_table"


def test_dynamic_table_full_refresh(project, my_dynamic_table):
    run_dbt(["run", "--models", my_dynamic_table.name])
    _, logs = run_dbt_and_capture(
        ["--debug", "run", "--models", my_dynamic_table.name, "--full-refresh"]
    )
    assert query_relation_type(project, my_dynamic_table) == "dynamic_table"
    assert_message_in_logs(f"Applying REPLACE to: {my_dynamic_table.fully_qualified_path}", logs)


def test_dynamic_table_replaces_table(project, my_dynamic_table, my_table):
    run_dbt(["run", "--models", my_table.name])
    sql = f"""
        alter table {my_table.fully_qualified_path}
        rename to {my_dynamic_table.name}
    """
    project.run_sql(sql)
    assert query_relation_type(project, my_dynamic_table) == "table"
    run_dbt(["run", "--models", my_dynamic_table.name])
    assert query_relation_type(project, my_dynamic_table) == "dynamic_table"


def test_dynamic_table_replaces_view(project, my_dynamic_table, my_view):
    run_dbt(["run", "--models", my_view.name])
    sql = f"""
        alter table {my_view.fully_qualified_path}
        rename to {my_dynamic_table.name}
    """
    project.run_sql(sql)
    assert query_relation_type(project, my_dynamic_table) == "view"
    run_dbt(["run", "--models", my_dynamic_table.name])
    assert query_relation_type(project, my_dynamic_table) == "dynamic_table"


def test_table_replaces_dynamic_table(project, my_dynamic_table):
    run_dbt(["run", "--models", my_dynamic_table.name])
    assert query_relation_type(project, my_dynamic_table) == "dynamic_table"
    swap_dynamic_table_to_table(project, my_dynamic_table)
    run_dbt(["run", "--models", my_dynamic_table.name])
    assert query_relation_type(project, my_dynamic_table) == "table"


def test_view_replaces_dynamic_table(project, my_dynamic_table):
    run_dbt(["run", "--models", my_dynamic_table.name])
    assert query_relation_type(project, my_dynamic_table) == "dynamic_table"
    swap_dynamic_table_to_view(project, my_dynamic_table)
    run_dbt(["run", "--models", my_dynamic_table.name])
    assert query_relation_type(project, my_dynamic_table) == "view"


def test_dynamic_table_only_updates_after_refresh(project, my_dynamic_table, my_seed):
    run_dbt(["run", "--models", my_dynamic_table.name])

    # poll database
    table_start = query_row_count(project, my_seed)
    view_start = query_row_count(project, my_dynamic_table)

    # insert new record in table
    project.run_sql(f"insert into {my_seed.fully_qualified_path} (id, value) values (4, 400);")

    # poll database
    table_mid = query_row_count(project, my_seed)
    view_mid = query_row_count(project, my_dynamic_table)

    # refresh the materialized view
    project.run_sql(f"alter dynamic table {my_dynamic_table.fully_qualified_path} refresh;")

    # poll database
    table_end = query_row_count(project, my_seed)
    view_end = query_row_count(project, my_dynamic_table)

    # new records were inserted in the table but didn't show up in the view until it was refreshed
    assert table_start < table_mid == table_end
    assert view_start == view_mid < view_end


class OnConfigurationChangeBase:
    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {"my_dynamic_table.sql": MY_DYNAMIC_TABLE}

    @pytest.fixture(scope="function", autouse=True)
    def setup(self, project, my_dynamic_table):
        run_dbt(["seed"])

        # make sure the model in the data reflects the files each time
        run_dbt(["run", "--models", my_dynamic_table.name, "--full-refresh"])

        # the tests touch these files, store their contents in memory
        initial_model = get_model_file(project, my_dynamic_table)

        yield

        # and then reset them after the test runs
        set_model_file(project, my_dynamic_table, initial_model)


class TestOnConfigurationChangeApply(OnConfigurationChangeBase):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"on_configuration_change": OnConfigurationChangeOption.Apply.value}}

    def test_target_lag_change_is_applied_with_alter(self, project, my_dynamic_table):
        """
        There are two key assertions that are commented out below. The issue is that we cannot
        query Snowflake to get metadata about dynamic tables without running two queries in the same
        call. The adapter is capable of doing this; it successfully runs `describe_template` on a
        dynamic table, which is exactly what we need. However, our test framework is not able to run
        that same query. The log checks have been kept as some level of testing and the two assertions
        were verified manually via a debugging break. The test framework issue should be resolved as
        soon as possible to remove this risk.
        """
        # assert query_target_lag(project, my_dynamic_table) == '2 minutes'
        swap_target_lag(project, my_dynamic_table)
        _, logs = run_dbt_and_capture(["--debug", "run", "--models", my_dynamic_table.name])
        # assert query_target_lag(project, my_dynamic_table) == '5 minutes'
        assert_message_in_logs(f"Applying ALTER to: {my_dynamic_table.fully_qualified_path}", logs)
        assert_message_in_logs(
            f"Applying UPDATE TARGET_LAG to: {my_dynamic_table.fully_qualified_path}", logs
        )
        assert_message_in_logs(
            f"Applying REPLACE to: {my_dynamic_table.fully_qualified_path}", logs, False
        )


class TestOnConfigurationChangeContinue(OnConfigurationChangeBase):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"on_configuration_change": OnConfigurationChangeOption.Continue.value}}

    def test_target_lag_change_is_not_applied(self, project, my_dynamic_table):
        """
        Please refer to `TestOnConfigurationChangeApply.test_target_lag_change_is_applied_with_alter`
        regarding the two commented assertions below.
        """
        # assert query_target_lag(project, my_dynamic_table) == '2 minutes'
        swap_target_lag(project, my_dynamic_table)
        _, logs = run_dbt_and_capture(["--debug", "run", "--models", my_dynamic_table.name])
        # assert query_target_lag(project, my_dynamic_table) == '2 minutes'
        assert_message_in_logs(
            f"Configuration changes were identified and `on_configuration_change` was set"
            f" to `continue` for `{my_dynamic_table.fully_qualified_path}`",
            logs,
        )
        assert_message_in_logs(
            f"Applying ALTER to: {my_dynamic_table.fully_qualified_path}", logs, False
        )
        assert_message_in_logs(
            f"Applying UPDATE TARGET_LAG to: {my_dynamic_table.fully_qualified_path}",
            logs,
            False,
        )
        assert_message_in_logs(
            f"Applying REPLACE to: {my_dynamic_table.fully_qualified_path}", logs, False
        )

    def test_full_refresh_still_occurs_with_changes(self, project, my_dynamic_table):
        run_dbt(["run", "--models", my_dynamic_table.name, "--full-refresh"])
        assert query_relation_type(project, my_dynamic_table) == "dynamic_table"


class TestOnConfigurationChangeFail(OnConfigurationChangeBase):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"on_configuration_change": OnConfigurationChangeOption.Fail.value}}

    def test_target_lag_change_is_not_applied(self, project, my_dynamic_table):
        """
        Please refer to `TestOnConfigurationChangeApply.test_target_lag_change_is_applied_with_alter`
        regarding the two commented assertions below.
        """
        # assert query_target_lag(project, my_dynamic_table) == '2 minutes'
        swap_target_lag(project, my_dynamic_table)
        # note the expected fail, versus the pass for `continue`
        _, logs = run_dbt_and_capture(
            ["--debug", "run", "--models", my_dynamic_table.name], expect_pass=False
        )
        # assert query_target_lag(project, my_dynamic_table) == '2 minutes'
        assert_message_in_logs(
            f"Configuration changes were identified and `on_configuration_change` was set"
            f" to `fail` for `{my_dynamic_table.fully_qualified_path}`",
            logs,
        )
        assert_message_in_logs(
            f"Applying ALTER to: {my_dynamic_table.fully_qualified_path}", logs, False
        )
        assert_message_in_logs(
            f"Applying UPDATE TARGET_LAG to: {my_dynamic_table.fully_qualified_path}",
            logs,
            False,
        )
        assert_message_in_logs(
            f"Applying REPLACE to: {my_dynamic_table.fully_qualified_path}", logs, False
        )

    def test_full_refresh_still_occurs_with_changes(self, project, my_dynamic_table):
        run_dbt(["run", "--models", my_dynamic_table.name, "--full-refresh"])
        assert query_relation_type(project, my_dynamic_table) == "dynamic_table"
