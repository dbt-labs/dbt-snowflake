from dbt.tests.adapter.store_test_failures_tests import basic
from dbt.tests.adapter.store_test_failures_tests.test_store_test_failures import (
    TestStoreTestFailures,
)


class TestSnowflakeStoreTestFailures(TestStoreTestFailures):
    pass


class TestStoreTestFailuresAsInteractions(basic.StoreTestFailuresAsInteractions):
    pass


class TestStoreTestFailuresAsProjectLevelOff(basic.StoreTestFailuresAsProjectLevelOff):
    pass


class TestStoreTestFailuresAsProjectLevelView(basic.StoreTestFailuresAsProjectLevelView):
    pass


class TestStoreTestFailuresAsGeneric(basic.StoreTestFailuresAsGeneric):
    pass


class TestStoreTestFailuresAsProjectLevelEphemeral(basic.StoreTestFailuresAsProjectLevelEphemeral):
    pass


class TestStoreTestFailuresAsExceptions(basic.StoreTestFailuresAsExceptions):
    pass
