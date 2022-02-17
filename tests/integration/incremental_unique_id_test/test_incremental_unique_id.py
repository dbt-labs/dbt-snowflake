from dbt.exceptions import ParsingException
from tests.integration.base import DBTIntegrationTest, use_profile
from pathlib import Path


class TestIncrementalUniqueKey(DBTIntegrationTest):
    @property
    def schema(self):
        return 'incremental_unique_key'

    @property
    def models(self):
        return 'models'

    def build_test_case(
        self, seed, incremental_model, update_sql_file, seed_expected_row_count
    ):
        '''Make incremental model from seed, then change seed'''
        seeds = self.run_dbt(['seed', '--full-refresh'])
        self.assertEqual(len(seeds), 1)

        models = self.run_dbt([
            'run', '--select', incremental_model, '--full-refresh'
        ])
        self.assertEqual(len(models), 1)

        if update_sql_file:
            self.run_sql_file(Path('seeds') / Path(update_sql_file + '.sql'))

        get_row_count = 'select * from {}.{}'.format(self.unique_schema(), seed)
        self.assertEqual(
            seed_expected_row_count,
            len(self.run_sql(get_row_count, fetch='all'))
        )

    def run_incremental_update(self, incremental_model):
        '''Attempt to update incremental model'''
        models = self.run_dbt(['run', '--select', incremental_model])
        self.assertEqual(len(models), 1)

    @use_profile('snowflake')
    def test__snowflake_no_unique_keys(self):
        '''with no unique keys, seed and model should match'''
        incremental_model='no_unique_key'

        self.build_test_case(
            seed='seed', incremental_model=incremental_model,
            update_sql_file='add_new_rows', seed_expected_row_count=8
        )
        self.run_incremental_update(incremental_model=incremental_model)

        self.assertTablesEqual('seed', incremental_model)


class TestIncrementalStrUniqueKey(TestIncrementalUniqueKey):
    @use_profile('snowflake')
    def test__snowflake_empty_str_unique_key(self):
        '''with empty string for unique key, seed and model should match'''
        incremental_model='empty_str_unique_key'

        self.build_test_case(
            seed='seed', incremental_model=incremental_model,
            update_sql_file='add_new_rows', seed_expected_row_count=8
        )
        self.run_incremental_update(incremental_model=incremental_model)

        self.assertTablesEqual('seed', incremental_model)

    @use_profile('snowflake')
    def test__snowflake_one_unique_key(self):
        '''with one unique key, model will overwritte existing row'''
        incremental_model='str_unique_key'
        expected_model='one_str_expected'

        self.build_test_case(
            seed='seed', incremental_model=incremental_model,
            update_sql_file='duplicate_insert', seed_expected_row_count=7
        )
        self.run_incremental_update(incremental_model=incremental_model)

        models = self.run_dbt(['run', '--select', expected_model])
        self.assertEqual(len(models), 1)

        self.assertTablesEqual(expected_model, incremental_model)

    @use_profile('snowflake')
    def test__snowflake_badstr_unique_key(self):
        with self.assertRaises(AssertionError) as exc:
            self.run_dbt(['run', '--select', 'not_found_unique_key'])


class TestIncrementalListUniqueKey(TestIncrementalUniqueKey):
    pass

# no element list
# one element list
# three element list
# duplicate on one element
# one found, one not found
