import pytest

from dbt.tests.adapter.seed.test_seed import (
    BasicSeedTests,
    SeedConfigFullRefreshOff,
    SeedCustomSchema,
    SimpleSeedEnabledViaConfig,
    SeedParsing,
    SimpleSeedWithBOM,
    SeedSpecificFormats
)
from dbt.tests.util import (
    run_dbt,
    read_file
)
from dbt.tests.adapter.seed.test_seed_type_override import SimpleSeedColumnOverride
from fixtures_local import properties__schema_yml


class TestBasicSeedTests(BasicSeedTests):
    def test_simple_seed_full_refresh_flag(self, project):
        '''Snowflake does not automatically truncate dependent views.'''
        pass


class TestSeedConfigFullRefreshOff(SeedConfigFullRefreshOff):
    pass


class TestSeedCustomSchema(SeedCustomSchema):
    pass


class TestSimpleSeedEnabledViaConfig(SimpleSeedEnabledViaConfig):
    pass


class TestSeedParsing(SeedParsing):
    pass


class TestSimpleSeedWithBOM(SimpleSeedWithBOM):
    pass


class TestSeedSpecificFormats(SeedSpecificFormats):
    @pytest.fixture(scope="class")
    def seeds(self, test_data_dir):
        seed_unicode = read_file(test_data_dir, "seed_unicode.csv")
        big_seed = read_file(self._make_big_seed(test_data_dir))

        return {
            "big_seed.csv": big_seed,
            "seed_unicode.csv": seed_unicode,
        }

    def test_simple_seed(self, project):
        results = run_dbt(["seed"])
        len(results) == 2


class TestSimpleSeedColumnOverride(SimpleSeedColumnOverride):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": properties__schema_yml,
        }

    def seed_enabled_types(self):
        return {
            "seed_id": "FLOAT",
            "birthday": "TEXT",
        }

    def seed_tricky_types(self):
        return {
            'seed_id_str': 'TEXT',
            'looks_like_a_bool': 'TEXT',
            'looks_like_a_date': 'TEXT',
        }
