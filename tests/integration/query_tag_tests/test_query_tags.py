import os
import csv
from tests.integration.base import DBTIntegrationTest, use_profile

class TestSeedWithQueryTag(DBTIntegrationTest):
    @property
    def schema(self):
        return "query_tag"

    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'models': {
                'test': {
                    'query_tag': self.prefix,
                    'post-hook': '{{ check_query_tag() }}'
                },
            },
            'seeds': {
                'test': {
                    'query_tag': self.prefix,
                    'post-hook': '{{ check_query_tag() }}'
                },
            },
            'snapshots': {
                'test': {
                    'query_tag': self.prefix,
                    'post-hook': '{{ check_query_tag() }}'
                },
            },
        }
        
    def build_all_with_query_tags(self):
        self.run_dbt(['build', '--vars', '{{"query_tag": "{}"}}'.format(self.prefix)])

    @use_profile('snowflake')
    def test__snowflake__build_tagged_twice(self):
        self.build_all_with_query_tags()
        self.build_all_with_query_tags()

    