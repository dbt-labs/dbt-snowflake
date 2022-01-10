import json

from tests.integration.base import DBTIntegrationTest, use_profile

class BaseTestColumnComment(DBTIntegrationTest):
    @property
    def schema(self):
        return "column_comment"

    @property
    def models(self):
        return "models"

    def run_has_comments(self):
        self.run_dbt()
        self.run_dbt(['docs', 'generate'])
        with open('target/catalog.json') as fp:
            catalog_data = json.load(fp)
        assert 'nodes' in catalog_data
        assert len(catalog_data['nodes']) == 1
        column_node = catalog_data['nodes']['model.test.quote_model']
        for column in column_node['columns'].keys():
            column_comment = column_node['columns'][column]['comment']
            assert column_comment.startswith('XXX')

class TestColumnCommentInTable(BaseTestColumnComment):
    @property
    def project_config(self):
        return {
            'config-version': 2,
            'models': {
                'test': {
                    'materialized': 'table',
                    '+persist_docs': {
                        "relation": True,
                        "columns": True,
                    },
                }
            }
        }
    @use_profile('snowflake')
    def test_snowflake_comments(self):
        self.run_has_comments()

class TestColumnCommentInView(BaseTestColumnComment):
    @property
    def project_config(self):
        return {
            'config-version': 2,
            'models': {
                'test': {
                    'materialized': 'view',
                    '+persist_docs': {
                        "relation": True,
                        "columns": True,
                    },
                }
            }
        }
    @use_profile('snowflake')
    def test_snowflake_comments(self):
        self.run_has_comments()
