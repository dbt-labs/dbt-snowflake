import pytest

from dbt.tests.adapter.materialized_views.test_basic import BasicTestsBase
from dbt.tests.adapter.materialized_views.test_on_configuration_change import (
    OnConfigurationChangeApplyTestsBase,
    OnConfigurationChangeSkipTestsBase,
    OnConfigurationChangeFailTestsBase,
)

from tests.functional.adapter.dynamic_table_tests.base import (
    SnowflakeBase,
    SnowflakeOnConfigurationChangeBase,
)


class TestBasic(SnowflakeBase, BasicTestsBase):
    pass


class TestOnConfigurationChangeApply(
    SnowflakeOnConfigurationChangeBase, OnConfigurationChangeApplyTestsBase
):
    @pytest.mark.skip(
        "This fails because there are no changes being made in the stubbed implementation"
    )
    def test_model_applies_changes_with_configuration_changes(
        self, project, configuration_changes_apply
    ):
        pass

    @pytest.mark.skip(
        "This fails because there are no monitored changes that trigger a full refresh"
    )
    def test_full_refresh_configuration_changes_will_not_attempt_apply_configuration_changes(
        self, project, configuration_changes_apply, configuration_changes_full_refresh
    ):
        pass


class TestOnConfigurationChangeSkip(
    SnowflakeOnConfigurationChangeBase, OnConfigurationChangeSkipTestsBase
):
    pass


class TestOnConfigurationChangeFail(
    SnowflakeOnConfigurationChangeBase, OnConfigurationChangeFailTestsBase
):
    pass
