import pytest
from dbt.tests.util import (
    run_dbt,
    check_relations_equal
)
from tests.functional.warehouse_test.fixtures import (
    models,
    project_config_models,
    project_files
    ) # noqa: F401
import os


class TestModelWarehouse():
    def dir(value):
        return os.path.normpath(value)

    @pytest.fixture(scope="class")
    def model_path(self):
        return self.dir('models')

    def test_snowflake_override_ok(self, project):
        run_dbt([
            'run',
            '--models', 'override_warehouse', 'expected_warehouse',
        ])
        check_relations_equal(project.adapter, ['OVERRIDE_WAREHOUSE', 'EXPECTED_WAREHOUSE'])

    def test_snowflake_override_noexist(self, project):
        run_dbt(['run', '--models', 'invalid_warehouse'], expect_pass=False)


class TestConfigWarehouse():
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            'config-version': 2,
            'model-paths': ['project-config-models'],
            'models': {
                'test': {
                    'snowflake_warehouse': os.getenv('SNOWFLAKE_TEST_ALT_WAREHOUSE', 'DBT_TEST_ALT'),
                },
            },
        }

    def dir(value):
        return os.path.normpath(value)

    @pytest.fixture(scope="class")
    def model_path(self):
        return self.dir('models')

    def test_snowflake_override_ok(self, project):
        run_dbt([
            'run',
            '--models', 'override_warehouse', 'expected_warehouse',
        ])
        check_relations_equal(project.adapter, ['OVERRIDE_WAREHOUSE', 'EXPECTED_WAREHOUSE'])
