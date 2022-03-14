from tests.integration.base import DBTIntegrationTest, use_profile


class TestBaseCaching(DBTIntegrationTest):
    @property
    def schema(self):
        return "caching"

    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'test-paths': ['tests']
        }

    @use_profile('snowflake')
    def test_snowflake_adapter_methods(self):
        self.run_dbt(['compile'])
        self.run_dbt()
        self.assertTablesEqual('MODEL', 'EXPECTED')

    @use_profile('snowflake')
    def test_snowflake_adapter_query_id(self):
        results = self.run_dbt()
        # strip to ensure nonempty string doesn't get passed
        assert results[0].adapter_response['query_id'].strip() != ""
