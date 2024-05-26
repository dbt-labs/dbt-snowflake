import pytest
import sys
import importlib
import dbt


@pytest.fixture
def remove_agate_from_path():
    """conftest and other envs load agate modules upon initilization so we need
    to remove their presence from module tracking to assess correctness of
    direct imports"""

    # import ahead of time to avoid reimporting agate upon package
    # initialization
    import dbt.adapters.snowflake.__init__

    original_sys_modules = sys.modules.copy()
    modules_to_remove = [m for m in sys.modules if "agate" in m]
    for m in modules_to_remove:
        del sys.modules[m]

    yield
    sys.modules = original_sys_modules


def test_lazy_loading_agate(remove_agate_from_path):
    """If agate is imported directly here or in any of the subsequent files,
    this test will fail. Also test that our assumptions about imports affecting
    sys modules hold.

    Because other tests use specific overrides of Agate, we can't import agate here
    directly unless the test is isolated onto its own process (via -n).
    """

    importlib.reload(dbt.adapters.snowflake.connections)
    importlib.reload(dbt.adapters.snowflake.impl)
    importlib.reload(dbt.adapters.snowflake.relation_configs.base)
    importlib.reload(dbt.adapters.snowflake.relation_configs.dynamic_table)
    assert not any([module_name for module_name in sys.modules if "agate" in module_name])
