import pathlib

import pytest


TEST_GROUPS = {
    "catalog_tests": "group_1",
    "column_types": "group_1",
    "custom_schema_tests": "group_1",
    "dbt_clone": "group_1",
    "dbt_show": "group_1",
    "empty": "group_1",
    "incremental": "group_2",
    "list_relations_tests": "group_2",
    "python_model_tests": "group_2",
    "query_comment_tests": "group_2",
    "simple_copy": "group_2",
    "simple_seed": "group_2",
    "statement_test": "group_3",
    "store_test_failures_tests": "group_3",
    "unit_testing": "group_3",
    "utils": "group_3",
    "test_aliases.py": "group_3",
    "test_anonymous_usage_stats.py": "group_3",
    "test_basic.py": "group_3",
    "test_caching.py": "group_4",
    "test_changing_relation_type.py": "group_4",
    "test_concurrency.py": "group_4",
    "test_constraints.py": "group_4",
    "test_ephemeral.py": "group_4",
    "test_get_last_relation_modified.py": "group_4",
    "test_grants.py": "group_5",
    "test_incremental_microbatch.py": "group_5",
    "test_persist_docs.py": "group_5",
    "test_python_model.py": "group_5",
    "test_simple_snapshot.py": "group_5",
    "test_timestamps.py": "group_5",
    "auth_tests": "group_6",
    "generic_test_tests": "group_6",
    "iceberg": "group_6",
    "override_database": "group_6",
    "query_tag": "group_6",
    "redact_log_values": "group_6",
    "relation_tests": "group_6",
    "snowflake_view_dependency": "group_6",
    "warehouse_test": "group_6",
    "test_isolated_begin_commit.py": "group_6",
}


def pytest_collection_modifyitems(config, items):
    test_root = pathlib.Path(config.rootdir) / "tests" / "functional"
    for item in items:
        test_path = pathlib.Path(item.fspath).relative_to(test_root)
        test_module = test_path.parts[0]
        if test_module == "adapter":
            test_module = test_path.parts[1]
        if mark_name := TEST_GROUPS.get(test_module):
            mark = getattr(pytest.mark, mark_name)
            item.add_marker(mark)
