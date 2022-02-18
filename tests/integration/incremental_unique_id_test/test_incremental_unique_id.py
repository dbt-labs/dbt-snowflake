from dbt.exceptions import ParsingException
from tests.integration.base import DBTIntegrationTest, use_profile
from collections import namedtuple
from pathlib import Path


TestResults = namedtuple(
    'TestResults',
    ['seed_count', 'model_count', 'seed_rows', 'inc_test_model_count',
     'opt_model_count', 'relation'],
)


class TestIncrementalUniqueKey(DBTIntegrationTest):
    @property
    def schema(self):
        return 'incremental_unique_key'

    @property
    def models(self):
        return 'models'

    def count_test_relations(self, seed, incremental_model):
        '''Idempotently create some number of seeds and incremental models'''
        seed_result_set = self.run_dbt(
            ['seed', '--select', seed, '--full-refresh']
        )
        model_result_set = self.run_dbt(
            ['run', '--select', incremental_model, '--full-refresh']
        )
        return len(seed_result_set), len(model_result_set)

    def get_seed_rows_after_update(self, seed, update_sql_file):
        '''Upate seed and return new row count'''
        row_count_query = 'select * from {}.{}'.format(
            self.unique_schema(),
            seed
        )

        self.run_sql_file(Path('seeds') / Path(update_sql_file + '.sql'))
        return len(self.run_sql(row_count_query, fetch='all'))

    def update_incremental_model(self, incremental_model):
        '''update incremental model after the seed table has been updated'''
        model_result_set = self.run_dbt(['run', '--select', incremental_model])
        return len(model_result_set)

    def setup_test(self, seed, incremental_model, update_sql_file):
        '''build a test case and return values for assertions'''
        (seed_count, model_count) = self.count_test_relations(
            seed=seed, incremental_model=incremental_model
        )
        seed_rows = self.get_seed_rows_after_update(
            seed=seed, update_sql_file=update_sql_file
        )
        inc_test_model_count = self.update_incremental_model(
            incremental_model=incremental_model
        )

        return (seed_count, model_count, seed_rows, inc_test_model_count)

    def run_test(self, expected_fields, test_case_fields):
        '''Invoke assertions to verify correct build functionality'''
        # 1. test seed(s) should build afresh
        self.assertEqual(
            expected_fields.seed_count, test_case_fields.seed_count
        )
        # 2. test model(s) should build afresh
        self.assertEqual(
            expected_fields.model_count, test_case_fields.model_count
        )
        # 3. seeds should have intended row counts post update
        self.assertEqual(
            expected_fields.seed_rows, test_case_fields.seed_rows
        )
        # 4. incremental test model(s) should be updated
        self.assertEqual(
            expected_fields.inc_test_model_count,
            test_case_fields.inc_test_model_count
        )
        # 5. extra incremental model(s) should be built; optional since
        #   comparison may be between an incremental model and seed
        if (expected_fields.opt_model_count and
            test_case_fields.opt_model_count):
            self.assertEqual(
                expected_fields.opt_model_count,
                test_case_fields.opt_model_count
            )
        # 6. result table should match intended result set (itself a relation)
        self.assertTablesEqual(
            expected_fields.relation, test_case_fields.relation
        )

    def stub_expected_fields(
        self, relation, seed_rows, opt_model_count=None
    ):
        return TestResults(
            seed_count=1, model_count=1, seed_rows=seed_rows,
            inc_test_model_count=1, opt_model_count=opt_model_count,
            relation=relation
        )

class TestIncrementalWithoutUniqueKey(TestIncrementalUniqueKey):
    @use_profile('snowflake')
    def test__snowflake_no_unique_keys(self):
        '''with no unique keys, seed and model should match'''
        seed='seed'
        seed_rows=8
        incremental_model='no_unique_key'
        update_sql_file='add_new_rows'

        expected_fields = self.stub_expected_fields(
            relation=seed, seed_rows=seed_rows
        )
        test_case_fields = TestResults(
            *self.setup_test(seed, incremental_model, update_sql_file),
            opt_model_count=None, relation=incremental_model
        )

        self.run_test(expected_fields, test_case_fields)

class TestIncrementalStrUniqueKey(TestIncrementalUniqueKey):
    @use_profile('snowflake')
    def test__snowflake_empty_str_unique_key(self):
        '''with empty string for unique key, seed and model should match'''
        seed='seed'
        seed_rows=8
        incremental_model='empty_str_unique_key'
        update_sql_file='add_new_rows'

        expected_fields = self.stub_expected_fields(
            relation=seed, seed_rows=seed_rows
        )
        test_case_fields = TestResults(
            *self.setup_test(seed, incremental_model, update_sql_file),
            opt_model_count=None, relation=incremental_model
        )

        self.run_test(expected_fields, test_case_fields)

    @use_profile('snowflake')
    def test__snowflake_one_unique_key(self):
        '''with one unique key, model will overwrite existing row'''
        seed='seed'
        seed_rows=7
        incremental_model='str_unique_key'
        update_sql_file='duplicate_insert'
        expected_model='one_str__overwrite'

        expected_fields = self.stub_expected_fields(
            relation=expected_model, seed_rows=seed_rows, opt_model_count=1
        )
        test_case_fields = TestResults(
            *self.setup_test(seed, incremental_model, update_sql_file),
            opt_model_count=self.update_incremental_model(expected_model),
            relation=incremental_model
        )

        self.run_test(expected_fields, test_case_fields)

    @use_profile('snowflake')
    def test__snowflake_bad_unique_key(self):
        '''using a unique key not in seed or derived CTEs leads to an error'''
        with self.assertRaises(AssertionError) as exc:
            self.run_dbt(['run', '--select', 'not_found_unique_key'])


class TestIncrementalListUniqueKey(TestIncrementalUniqueKey):
    @use_profile('snowflake')
    def test__snowflake_empty_unique_key_list(self):
        '''with no unique keys, seed and model should match'''
        seed='seed'
        seed_rows=8
        incremental_model='empty_unique_key_list'
        update_sql_file='add_new_rows'

        expected_fields = self.stub_expected_fields(
            relation=seed, seed_rows=seed_rows
        )
        test_case_fields = TestResults(
            *self.setup_test(seed, incremental_model, update_sql_file),
            opt_model_count=None, relation=incremental_model
        )

        self.run_test(expected_fields, test_case_fields)

    @use_profile('snowflake')
    def test__snowflake_unary_unique_key_list(self):
        '''with one unique key, model will overwrite existing row'''
        seed='seed'
        seed_rows=7
        incremental_model='unary_unique_key_list'
        update_sql_file='duplicate_insert'
        expected_model='unique_key_list__inplace_overwrite'

        expected_fields = self.stub_expected_fields(
            relation=expected_model, seed_rows=seed_rows, opt_model_count=1
        )
        test_case_fields = TestResults(
            *self.setup_test(seed, incremental_model, update_sql_file),
            opt_model_count=self.update_incremental_model(expected_model),
            relation=incremental_model
        )

        self.run_test(expected_fields, test_case_fields)

    @use_profile('snowflake')
    def test__snowflake_duplicated_unary_unique_key_list(self):
        '''with two of the same unique key, model will overwrite existing row'''
        seed='seed'
        seed_rows=7
        incremental_model='duplicated_unary_unique_key_list'
        update_sql_file='duplicate_insert'
        expected_model='unique_key_list__inplace_overwrite'

        expected_fields = self.stub_expected_fields(
            relation=expected_model, seed_rows=seed_rows, opt_model_count=1
        )
        test_case_fields = TestResults(
            *self.setup_test(seed, incremental_model, update_sql_file),
            opt_model_count=self.update_incremental_model(expected_model),
            relation=incremental_model
        )

        self.run_test(expected_fields, test_case_fields)

    @use_profile('snowflake')
    def test__snowflake_trinary_unique_key_list(self):
        '''with three unique keys, model will overwrite existing row'''
        seed='seed'
        seed_rows=7
        incremental_model='trinary_unique_key_list'
        update_sql_file='duplicate_insert'
        expected_model='unique_key_list__inplace_overwrite'

        expected_fields = self.stub_expected_fields(
            relation=expected_model, seed_rows=seed_rows, opt_model_count=1
        )
        test_case_fields = TestResults(
            *self.setup_test(seed, incremental_model, update_sql_file),
            opt_model_count=self.update_incremental_model(expected_model),
            relation=incremental_model
        )

        self.run_test(expected_fields, test_case_fields)

    @use_profile('snowflake')
    def test__snowflake_trinary_unique_key_list_no_update(self):
        '''even with three unique keys, adding distinct rows to seed does not
           cause seed and model to diverge'''
        seed='seed'
        seed_rows=8
        incremental_model='nontyped_trinary_unique_key_list'
        update_sql_file='add_new_rows'

        expected_fields = self.stub_expected_fields(
            relation=seed, seed_rows=seed_rows
        )
        test_case_fields = TestResults(
            *self.setup_test(seed, incremental_model, update_sql_file),
            opt_model_count=None, relation=incremental_model
        )

        self.run_test(expected_fields, test_case_fields)

    @use_profile('snowflake')
    def test__snowflake_bad_unique_key_list(self):
        '''using a unique key not in seed or derived CTEs leads to an error'''
        with self.assertRaises(AssertionError) as exc:
            self.run_dbt(['run', '--select', 'not_found_unique_key_list'])
