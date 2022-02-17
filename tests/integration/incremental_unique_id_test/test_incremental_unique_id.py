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
        '''update incremental model after the seed table has been updated'''
        models = self.run_dbt(['run', '--select', incremental_model])
        self.assertEqual(len(models), 1)

    def run_incremental_mirror_seed_test(
        self, incremental_model, seed, seed_expected_row_count
    ):
        '''invoke idempotent seed and model build with no model overwrite'''
        self.build_test_case(
            seed=seed,
            incremental_model=incremental_model,
            update_sql_file='add_new_rows',
            seed_expected_row_count=seed_expected_row_count
        )
        self.run_incremental_update(incremental_model=incremental_model)

        self.assertTablesEqual(seed, incremental_model)

    def run_incremental_match_test(
        self, incremental_model, update_sql_file, expected_model,
        seed_expected_row_count
    ):
        '''invoke idempotent model seed and build with model overwrite'''
        self.build_test_case(
            seed='seed',
            incremental_model=incremental_model,
            update_sql_file=update_sql_file,
            seed_expected_row_count=seed_expected_row_count
        )
        self.run_incremental_update(incremental_model=incremental_model)

        models = self.run_dbt(['run', '--select', expected_model])
        self.assertEqual(len(models), 1)

        self.assertTablesEqual(expected_model, incremental_model)

    @use_profile('snowflake')
    def test__snowflake_no_unique_keys(self):
        '''with no unique keys, seed and model should match'''
        self.run_incremental_mirror_seed_test(
            incremental_model='no_unique_key',
            seed='seed',
            seed_expected_row_count=8
        )


class TestIncrementalStrUniqueKey(TestIncrementalUniqueKey):
    @use_profile('snowflake')
    def test__snowflake_empty_str_unique_key(self):
        '''with empty string for unique key, seed and model should match'''
        self.run_incremental_mirror_seed_test(
            incremental_model='empty_str_unique_key',
            seed='seed',
            seed_expected_row_count=8
        )

    @use_profile('snowflake')
    def test__snowflake_one_unique_key(self):
        '''with one unique key, model will overwrite existing row'''
        self.run_incremental_match_test(
            incremental_model='str_unique_key',
            update_sql_file='duplicate_insert',
            expected_model='one_str__overwrite',
            seed_expected_row_count=7
        )

    @use_profile('snowflake')
    def test__snowflake_bad_unique_key(self):
        '''using a unique key not in seed or derived CTEs leads to an error'''
        with self.assertRaises(AssertionError) as exc:
            self.run_dbt(['run', '--select', 'not_found_unique_key'])


class TestIncrementalListUniqueKey(TestIncrementalUniqueKey):
    @use_profile('snowflake')
    def test__snowflake_empty_unique_key_list(self):
        '''with no unique keys, seed and model should match'''
        self.run_incremental_mirror_seed_test(
            incremental_model='empty_unique_key_list',
            seed='seed',
            seed_expected_row_count=8
        )

    @use_profile('snowflake')
    def test__snowflake_unary_unique_key_list(self):
        '''with one unique key, model will overwrite existing row'''
        self.run_incremental_match_test(
            incremental_model='unary_unique_key_list',
            update_sql_file='duplicate_insert',
            expected_model='unique_key_list__inplace_overwrite',
            seed_expected_row_count=7
        )

    @use_profile('snowflake')
    def test__snowflake_duplicated_unary_unique_key_list(self):
        '''with two of the same unique key, model will overwrite existing row'''
        self.run_incremental_match_test(
            incremental_model='duplicated_unary_unique_key_list',
            update_sql_file='duplicate_insert',
            expected_model='unique_key_list__inplace_overwrite',
            seed_expected_row_count=7
        )

    @use_profile('snowflake')
    def test__snowflake_trinary_unique_key_list(self):
        '''with three unique keys, model will overwrite existing row'''
        self.run_incremental_match_test(
            incremental_model='trinary_unique_key_list',
            update_sql_file='duplicate_insert',
            expected_model='unique_key_list__inplace_overwrite',
            seed_expected_row_count=7
        )

    @use_profile('snowflake')
    def test__snowflake_unique_key_list_no_update(self):
        '''with a fitting unique key, model will not overwrite existing row'''
        self.run_incremental_mirror_seed_test(
            incremental_model='nontyped_trinary_unique_key_list',
            seed='seed',
            seed_expected_row_count=8
        )

    @use_profile('snowflake')
    def test__snowflake_bad_unique_key_list(self):
        '''using a unique key not in seed or derived CTEs leads to an error'''
        with self.assertRaises(AssertionError) as exc:
            self.run_dbt(['run', '--select', 'not_found_unique_key_list'])
