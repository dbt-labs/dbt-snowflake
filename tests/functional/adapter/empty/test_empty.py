from dbt.tests.adapter.empty.test_empty import (
    BaseTestEmpty,
    BaseTestEmptyInlineSourceRef,
    MetadataWithEmptyFlag,
)


class TestSnowflakeEmpty(BaseTestEmpty):
    pass


class TestSnowflakeEmptyInlineSourceRef(BaseTestEmptyInlineSourceRef):
    pass


class TestMetadataWithEmptyFlag(MetadataWithEmptyFlag):
    pass
