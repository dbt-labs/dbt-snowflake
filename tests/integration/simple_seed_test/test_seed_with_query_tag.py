import os
import csv
from tests.integration.base import DBTIntegrationTest, use_profile

class TestSeedWithQueryTag(DBTIntegrationTest):
    @property
    def schema(self):
        return "simple_seed"

    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            "seed-paths": ['seeds-config'],
            "test-paths": ['check-query-tag-expected'],
            'seeds': {
                'test': {
                    'enabled': False,
                    'quote_columns': True,
                    'query_tag': self.prefix,
                    'seed_enabled': {
                        'enabled': True,
                    },
                },
                
            }
        }

    def assert_query_tag_expected(self):
        self.run_dbt(['test', '--select', 'test_type:singular', '--vars', '{{"query_tag": {}}}'.format(self.prefix)])

    @use_profile('snowflake')
    def test_snowflake_big_batched_seed(self):
        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)
        self.assert_query_tag_expected()
    