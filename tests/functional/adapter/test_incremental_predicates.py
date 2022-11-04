import pytest
from dbt.tests.adapter.incremental.test_incremental_predicates import BaseIncrementalPredicates


class TestIncrementalPredicatesDeleteInsertSnowflake(BaseIncrementalPredicates):
    pass


class TestIncrementalPredicatesMergeSnowflake(BaseIncrementalPredicates):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": { 
                "+incremental_predicates": [
                    "dbt_internal_dest.id != 2"
                ],
                "+incremental_strategy": "merge"
            }
        }