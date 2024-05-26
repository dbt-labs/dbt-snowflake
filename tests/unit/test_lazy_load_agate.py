import pytest
import sys
import importlib
import dbt
import copy


def test_lazy_loading_agate():
    """If agate is imported directly here or in any of the subsequent files, this test will fail. Also test that our assumptions about imports affecting sys modules hold.

    Because other tests use Agate, we must be extra careful with state changes.
    """

    # import ahead of time to avoid reimporting agate upon package initialization

    original_modules = copy.copy(sys.modules)
    try:
        agate_modules = [m for m in sys.modules if "agate" in m]
        for m in agate_modules:
            del sys.modules[m]

        import dbt.adapters.snowflake.__init__

        importlib.reload(dbt.adapters.snowflake.connections)
        importlib.reload(dbt.adapters.snowflake.impl)
        importlib.reload(dbt.adapters.snowflake.relation_configs.base)
        importlib.reload(dbt.adapters.snowflake.relation_configs.dynamic_table)
        assert not any([module_name for module_name in sys.modules if "agate" in module_name])

    finally:
        sys.modules = original_modules
