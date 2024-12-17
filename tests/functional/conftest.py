import pathlib

import pytest


TEST_GROUPS = {
    "catalog_tests": "last_year",
    "column_types": "last_year",
    "custom_schema_tests": "last_year",
    "dbt_clone": "last_quarter",
    "dbt_show": "last_quarter",
    "empty": "last_quarter",
    "incremental": "microbatch",
    "list_relations_tests": "last_month",
    "python_model_tests": "python_model",
    "query_comment_tests": "last_year",
    "simple_copy": "last_year",
    "simple_seed": "last_year",
    "statement_test": "last_year",
    "store_test_failures_tests": "last_year",
    "unit_testing": "last_year",
    "utils": "last_year",
    "test_aliases.py": "last_year",
    "test_anonymous_usage_stats.py": "last_year",
    "test_basic.py": "last_year",
    "test_caching.py": "last_year",
    "test_changing_relation_type.py": "large",
    "test_concurrency.py": "last_year",
    "test_constraints.py": "last_year",
    "test_ephemeral.py": "last_year",
    "test_get_last_relation_modified.py": "last_year",
    "test_grants.py": "last_year",
    "test_incremental_microbatch.py": "microbatch",
    "test_persist_docs.py": "last_year",
    "test_python_model.py": "python_model",
    "test_simple_snapshot.py": "last_year",
    "test_timestamps.py": "last_year",
    "auth_tests": "last_month",
    "generic_test_tests": "last_month",
    "iceberg": "last_month",
    "override_database": "last_year",
    "query_tag": "last_month",
    "redact_log_values": "last_year",
    "relation_tests": "last_month",
    "snowflake_view_dependency": "last_year",
    "warehouse_test": "last_quarter",
    "test_isolated_begin_commit.py": "last_year",
}


def pytest_collection_modifyitems(config, items):
    test_root = pathlib.Path(config.rootdir) / "tests" / "functional"
    for item in items:
        try:
            test_path = pathlib.Path(item.fspath).relative_to(test_root)
        except ValueError:
            continue
        test_module = test_path.parts[0]
        if test_module == "adapter":
            test_module = test_path.parts[1]
        if mark_name := TEST_GROUPS.get(test_module):
            mark = getattr(pytest.mark, mark_name)
            item.add_marker(mark)
