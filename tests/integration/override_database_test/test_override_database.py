from tests.integration.base import DBTIntegrationTest, use_profile

import os


class BaseOverrideDatabase(DBTIntegrationTest):
    setup_alternate_db = True

    @property
    def schema(self):
        return "override_database_040"

    @property
    def models(self):
        return "models"

    @property
    def alternative_database(self):
        if self.adapter_type == 'snowflake':
            return os.getenv('SNOWFLAKE_TEST_DATABASE')
        else:
            return super().alternative_database

    def snowflake_profile(self):
        return {
            'config': {
                'send_anonymous_usage_stats': False
            },
            'test': {
                'outputs': {
                    'default2': {
                        'type': 'snowflake',
                        'threads': 4,
                        'account': os.getenv('SNOWFLAKE_TEST_ACCOUNT'),
                        'user': os.getenv('SNOWFLAKE_TEST_USER'),
                        'password': os.getenv('SNOWFLAKE_TEST_PASSWORD'),
                        'database': os.getenv('SNOWFLAKE_TEST_ALT_DATABASE'),
                        'schema': self.unique_schema(),
                        'warehouse': os.getenv('SNOWFLAKE_TEST_WAREHOUSE'),
                    },
                    'noaccess': {
                        'type': 'snowflake',
                        'threads': 4,
                        'account': os.getenv('SNOWFLAKE_TEST_ACCOUNT'),
                        'user': 'noaccess',
                        'password': 'password',
                        'database': os.getenv('SNOWFLAKE_TEST_DATABASE'),
                        'schema': self.unique_schema(),
                        'warehouse': os.getenv('SNOWFLAKE_TEST_WAREHOUSE'),
                    }
                },
                'target': 'default2'
            }
        }

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'seed-paths': ['seeds'],
            'vars': {
                'alternate_db': self.alternative_database,
            },
            'quoting': {
                'database': True,
            },
            'seeds': {
                'quote_columns': False,
            }
        }


class TestModelOverride(BaseOverrideDatabase):
    def run_database_override(self):
        if self.adapter_type == 'snowflake':
            func = lambda x: x.upper()
        else:
            func = lambda x: x

        self.run_dbt(['seed'])

        self.assertEqual(len(self.run_dbt(['run'])), 4)
        self.assertManyRelationsEqual([
            (func('seed'), self.unique_schema(), self.default_database),
            (func('view_2'), self.unique_schema(), self.alternative_database),
            (func('view_1'), self.unique_schema(), self.default_database),
            (func('view_3'), self.unique_schema(), self.default_database),
            (func('view_4'), self.unique_schema(), self.alternative_database),
        ])

    @use_profile('snowflake')
    def test_snowflake_database_override(self):
        self.run_database_override()


class BaseTestProjectModelOverride(BaseOverrideDatabase):
    # this is janky, but I really want to access self.default_database in
    # project_config
    @property
    def default_database(self):
        target = self._profile_config['test']['target']
        profile = self._profile_config['test']['outputs'][target]
        for key in ['database', 'project', 'dbname']:
            if key in profile:
                database = profile[key]
                if self.adapter_type == 'snowflake':
                    return database.upper()
                return database
        assert False, 'No profile database found!'

    def run_database_override(self):
        self.run_dbt(['seed'])
        self.assertEqual(len(self.run_dbt(['run'])), 4)
        self.assertExpectedRelations()

    def assertExpectedRelations(self):
        if self.adapter_type == 'snowflake':
            func = lambda x: x.upper()
        else:
            func = lambda x: x

        self.assertManyRelationsEqual([
            (func('seed'), self.unique_schema(), self.default_database),
            (func('view_2'), self.unique_schema(), self.alternative_database),
            (func('view_1'), self.unique_schema(), self.alternative_database),
            (func('view_3'), self.unique_schema(), self.default_database),
            (func('view_4'), self.unique_schema(), self.alternative_database),
        ])


class TestProjectModelOverride(BaseTestProjectModelOverride):
    @property
    def project_config(self):
        return {
            'config-version': 2,
            'vars': {
                'alternate_db': self.alternative_database,
            },
            'models': {
                'database': self.alternative_database,
                'test': {
                    'subfolder': {
                        'database': self.default_database,
                    }
                }
            },
            'seed-paths': ['seeds'],
            'vars': {
                'alternate_db': self.alternative_database,
            },
            'quoting': {
                'database': True,
            },
            'seeds': {
                'quote_columns': False,
            }
        }

    @use_profile('snowflake')
    def test_snowflake_database_override(self):
        self.run_database_override()


class TestProjectModelAliasOverride(BaseTestProjectModelOverride):
    @property
    def project_config(self):
        return {
            'config-version': 2,
            'vars': {
                'alternate_db': self.alternative_database,
            },
            'models': {
                'project': self.alternative_database,
                'test': {
                    'subfolder': {
                        'project': self.default_database,
                    }
                }
            },
            'seed-paths': ['seeds'],
            'vars': {
                'alternate_db': self.alternative_database,
            },
            'quoting': {
                'database': True,
            },
            'seeds': {
                'quote_columns': False,
            }
        }


class TestProjectSeedOverride(BaseOverrideDatabase):
    def run_database_override(self):
        func = lambda x: x.upper()

        self.use_default_project({
            'config-version': 2,
            'seeds': {
                'database': self.alternative_database
            },
        })
        self.run_dbt(['seed'])

        self.assertEqual(len(self.run_dbt(['run'])), 4)
        self.assertManyRelationsEqual([
            (func('seed'), self.unique_schema(), self.alternative_database),
            (func('view_2'), self.unique_schema(), self.alternative_database),
            (func('view_1'), self.unique_schema(), self.default_database),
            (func('view_3'), self.unique_schema(), self.default_database),
            (func('view_4'), self.unique_schema(), self.alternative_database),
        ])

    @use_profile('snowflake')
    def test_snowflake_database_override(self):
        self.run_database_override()
