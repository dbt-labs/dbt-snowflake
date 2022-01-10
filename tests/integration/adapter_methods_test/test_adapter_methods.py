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
        self.run_dbt(['compile'])  # trigger any compile-time issues
        results = self.run_dbt()
        assert results[0].adapter_response['query_id'] is not None
        self.assertTablesEqual('MODEL', 'EXPECTED')