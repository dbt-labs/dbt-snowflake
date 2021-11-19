from tests.integration.base import DBTIntegrationTest, use_profile

class TestSnapshotWithQueryTag(DBTIntegrationTest):
    @property
    def schema(self):
        return "simple_snapshot_004"

    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            "snapshot-paths": ['check-snapshots-query-tag'],
            "test-paths": ['check-snapshots-query-tag-expected'],
            "model-paths": [],
        }

    def dbt_run_seed(self):
        self.run_sql_file('seed.sql')

    def test_snapshot_with_query_tag(self):
        self.run_dbt(["snapshot", "--vars", '{{"query_tag": {}}}'.format(self.prefix)])

    def assert_query_tag_expected(self):
        self.run_dbt(['test', '--select', 'test_type:singular', '--vars', '{{"query_tag": {}}}'.format(self.prefix)])

    @use_profile('snowflake')
    def test__snowflake__snapshot_with_query_tag(self):
        self.dbt_run_seed()
        self.test_snapshot_with_query_tag()
        self.assert_query_tag_expected()
