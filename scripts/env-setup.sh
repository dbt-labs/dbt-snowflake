#!/bin/bash
# Set TOXENV environment variable for subsequent steps
echo "TOXENV=integration-snowflake" >> $GITHUB_ENV
# Set INTEGRATION_TESTS_SECRETS_PREFIX environment variable for subsequent steps
# All GH secrets that have this prefix will be set as environment variables
echo "INTEGRATION_TESTS_SECRETS_PREFIX=SNOWFLAKE_TEST" >> $GITHUB_ENV
# Set environment variables required for integration tests
echo "DBT_TEST_USER_1=dbt_test_role_1" >> $GITHUB_ENV
echo "DBT_TEST_USER_2=dbt_test_role_2" >> $GITHUB_ENV
echo "DBT_TEST_USER_3=dbt_test_role_3" >> $GITHUB_ENV
