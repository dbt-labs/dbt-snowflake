import pytest

from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import (
    BaseSingularTestsEphemeral,
)
from dbt.tests.adapter.basic.test_get_catalog_for_single_relation import (
    BaseGetCatalogForSingleRelation,
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
from dbt_common.contracts.metadata import CatalogTable, TableMetadata, ColumnMetadata, StatsItem

from dbt.adapters.snowflake.relation_configs import SnowflakeRelationType
from tests.functional.adapter.expected_stats import snowflake_stats


class TestSimpleMaterializationsSnowflake(BaseSimpleMaterializations):
    pass


class TestSingularTestsSnowflake(BaseSingularTests):
    pass


class TestGetCatalogForSingleRelationSnowflake(BaseGetCatalogForSingleRelation):
    @pytest.fixture(scope="class")
    def current_role(self, project):
        return project.run_sql("select current_role()", fetch="one")[0]

    @pytest.fixture(scope="class")
    def expected_catalog_my_seed(self, project, current_role):
        return CatalogTable(
            metadata=TableMetadata(
                type=SnowflakeRelationType.Table.upper(),
                schema=project.test_schema.upper(),
                name="MY_SEED",
                database=project.database,
                comment="",
                owner=current_role,
            ),
            columns={
                "ID": ColumnMetadata(type="NUMBER", index=1, name="ID", comment=None),
                "FIRST_NAME": ColumnMetadata(
                    type="VARCHAR", index=2, name="FIRST_NAME", comment=None
                ),
                "EMAIL": ColumnMetadata(type="VARCHAR", index=3, name="EMAIL", comment=None),
                "IP_ADDRESS": ColumnMetadata(
                    type="VARCHAR", index=4, name="IP_ADDRESS", comment=None
                ),
                "UPDATED_AT": ColumnMetadata(
                    type="TIMESTAMP_NTZ", index=5, name="UPDATED_AT", comment=None
                ),
            },
            stats={
                "has_stats": StatsItem(
                    id="has_stats",
                    label="Has Stats?",
                    value=True,
                    include=False,
                    description="Indicates whether there are statistics for this table",
                ),
                "row_count": StatsItem(
                    id="row_count",
                    label="Row Count",
                    value=1,
                    include=True,
                    description="Number of rows in the table as reported by Snowflake",
                ),
                "bytes": StatsItem(
                    id="bytes",
                    label="Approximate Size",
                    value=2048,
                    include=True,
                    description="Size of the table as reported by Snowflake",
                ),
            },
            unique_id=None,
        )

    @pytest.fixture(scope="class")
    def expected_catalog_my_view_model(self, project, current_role):
        return CatalogTable(
            metadata=TableMetadata(
                type=SnowflakeRelationType.View.upper(),
                schema=project.test_schema.upper(),
                name="MY_VIEW_MODEL",
                database=project.database,
                comment="",
                owner=current_role,
            ),
            columns={
                "ID": ColumnMetadata(type="NUMBER", index=1, name="ID", comment=None),
                "FIRST_NAME": ColumnMetadata(
                    type="VARCHAR", index=2, name="FIRST_NAME", comment=None
                ),
                "EMAIL": ColumnMetadata(type="VARCHAR", index=3, name="EMAIL", comment=None),
                "IP_ADDRESS": ColumnMetadata(
                    type="VARCHAR", index=4, name="IP_ADDRESS", comment=None
                ),
                "UPDATED_AT": ColumnMetadata(
                    type="TIMESTAMP_NTZ", index=5, name="UPDATED_AT", comment=None
                ),
            },
            stats={
                "has_stats": StatsItem(
                    id="has_stats",
                    label="Has Stats?",
                    value=True,
                    include=False,
                    description="Indicates whether there are statistics for this table",
                ),
                "row_count": StatsItem(
                    id="row_count",
                    label="Row Count",
                    value=0,
                    include=False,
                    description="Number of rows in the table as reported by Snowflake",
                ),
                "bytes": StatsItem(
                    id="bytes",
                    label="Approximate Size",
                    value=0,
                    include=False,
                    description="Size of the table as reported by Snowflake",
                ),
            },
            unique_id=None,
        )

    @pytest.fixture(scope="class")
    def expected_catalog_my_table_model(self, project, current_role):
        return CatalogTable(
            metadata=TableMetadata(
                type=SnowflakeRelationType.Table.upper(),
                schema=project.test_schema.upper(),
                name="MY_TABLE_MODEL",
                database=project.database,
                comment="",
                owner=current_role,
            ),
            columns={
                "ID": ColumnMetadata(type="NUMBER", index=1, name="ID", comment=None),
                "FIRST_NAME": ColumnMetadata(
                    type="VARCHAR", index=2, name="FIRST_NAME", comment=None
                ),
                "EMAIL": ColumnMetadata(type="VARCHAR", index=3, name="EMAIL", comment=None),
                "IP_ADDRESS": ColumnMetadata(
                    type="VARCHAR", index=4, name="IP_ADDRESS", comment=None
                ),
                "UPDATED_AT": ColumnMetadata(
                    type="TIMESTAMP_NTZ", index=5, name="UPDATED_AT", comment=None
                ),
            },
            stats={
                "has_stats": StatsItem(
                    id="has_stats",
                    label="Has Stats?",
                    value=True,
                    include=False,
                    description="Indicates whether there are statistics for this table",
                ),
                "row_count": StatsItem(
                    id="row_count",
                    label="Row Count",
                    value=1,
                    include=True,
                    description="Number of rows in the table as reported by Snowflake",
                ),
                "bytes": StatsItem(
                    id="bytes",
                    label="Approximate Size",
                    value=2048,
                    include=True,
                    description="Size of the table as reported by Snowflake",
                ),
            },
            unique_id=None,
        )


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
