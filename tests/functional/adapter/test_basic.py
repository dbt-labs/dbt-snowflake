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
from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod
from dbt.tests.adapter.basic.test_docs_generate import BaseDocsGenerate
from dbt.tests.adapter.basic.expected_catalog import base_expected_catalog, no_stats
from tests.functional.adapter.expected_stats import snowflake_stats


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


class TestBaseAdapterMethodSnowflake(BaseAdapterMethod):
    @pytest.fixture(scope="class")
    def equal_tables(self):
        return ["MODEL", "EXPECTED"]


class TestDocsGenerateSnowflake(BaseDocsGenerate):
    @pytest.fixture(scope="class")
    def get_role(self, project):
        return project.run_sql("select current_role()", fetch="one")[0]

    @pytest.fixture(scope="class")
    def expected_catalog(self, project, get_role):
        return base_expected_catalog(
            project,
            role=get_role,
            id_type="NUMBER",
            text_type="TEXT",
            time_type="TIMESTAMP_NTZ",
            view_type="VIEW",
            table_type="BASE TABLE",
            model_stats=no_stats(),
            seed_stats=snowflake_stats(),
            case=lambda x: x.upper(),
            case_columns=False,
        )
