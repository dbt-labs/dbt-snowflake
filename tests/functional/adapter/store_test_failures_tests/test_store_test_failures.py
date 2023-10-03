from dbt.tests.adapter.store_test_failures_tests.basic import (
    StoreTestFailuresAsGeneric,
    StoreTestFailuresAsInteractions,
    StoreTestFailuresAsProjectLevelOff,
    StoreTestFailuresAsProjectLevelView,
)
from dbt.tests.adapter.store_test_failures_tests.test_store_test_failures import (
    TestStoreTestFailures,
)


class TestSnowflakeStoreTestFailures(TestStoreTestFailures):
    pass


class TestSnowflakeStoreTestFailuresAsInteractions(StoreTestFailuresAsInteractions):
    pass


class TestSnowflakeStoreTestFailuresAsProjectLevelOff(StoreTestFailuresAsProjectLevelOff):
    pass


class TestSnowflakeStoreTestFailuresAsProjectLevelView(StoreTestFailuresAsProjectLevelView):
    pass


class TestSnowflakeStoreTestFailuresAsGeneric(StoreTestFailuresAsGeneric):
    pass
