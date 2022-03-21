import pytest

from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import (
    BaseSingularTestsEphemeral,
)
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_incremental import BaseIncremental
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_snapshot_check_cols import BaseSnapshotCheckCols
from dbt.tests.adapter.basic.test_snapshot_timestamp import BaseSnapshotTimestamp


class TestSimpleMaterializationsSnowflake(BaseSimpleMaterializations):
    pass


class TestSingularTestsSnowflake(BaseSingularTests):
    pass


class TestSingularTestsEphemeralSnowflake(BaseSingularTestsEphemeral):
    pass


class TestEmptySnowflake(BaseEmpty):
    pass


class TestEphemeralSnowflake(BaseEphemeral):
    pass


class TestIncrementalSnowflake(BaseIncremental):
    pass


class TestGenericTestsSnowflake(BaseGenericTests):
    pass


class TestSnapshotCheckColsSnowflake(BaseSnapshotCheckCols):
    pass


class TestSnapshotTimestampSnowflake(BaseSnapshotTimestamp):
    pass
