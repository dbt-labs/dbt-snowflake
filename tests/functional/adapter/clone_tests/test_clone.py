import os
import shutil
from copy import deepcopy
import pytest
from dbt.tests.util import run_dbt
from tests.functional.adapter.clone_tests.fixtures import (
    seed_csv,
    table_model_sql,
    view_model_sql,
    ephemeral_model_sql,
    exposures_yml,
    schema_yml,
    snapshot_sql,
    get_schema_name_sql,
    macros_sql,
    infinite_macros_sql,
    custom_can_clone_tables_macros_sql,
)


class BaseClone:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table_model.sql": table_model_sql,
            "view_model.sql": view_model_sql,
            "ephemeral_model.sql": ephemeral_model_sql,
            "schema.yml": schema_yml,
            "exposures.yml": exposures_yml,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "macros.sql": macros_sql,
            "infinite_macros.sql": infinite_macros_sql,
            "get_schema_name.sql": get_schema_name_sql,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "seed.csv": seed_csv,
        }

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {
            "snapshot.sql": snapshot_sql,
        }

    @pytest.fixture(scope="class")
    def other_schema(self, unique_schema):
        return unique_schema + "_other"

    @property
    def project_config_update(self):
        return {
            "seeds": {
                "test": {
                    "quote_columns": False,
                }
            }
        }

    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target, unique_schema, other_schema):
        outputs = {"default": dbt_profile_target, "otherschema": deepcopy(dbt_profile_target)}
        outputs["default"]["schema"] = unique_schema
        outputs["otherschema"]["schema"] = other_schema
        return {"test": {"outputs": outputs, "target": "default"}}

    def copy_state(self, project_root):
        state_path = os.path.join(project_root, "state")
        if not os.path.exists(state_path):
            os.makedirs(state_path)
        shutil.copyfile(
            f"{project_root}/target/manifest.json", f"{project_root}/state/manifest.json"
        )

    def run_and_save_state(self, project_root, with_snapshot=False):
        results = run_dbt(["seed"])
        assert len(results) == 1
        assert not any(r.node.deferred for r in results)
        results = run_dbt(["run"])
        assert len(results) == 2
        assert not any(r.node.deferred for r in results)
        results = run_dbt(["test"])
        assert len(results) == 2

        if with_snapshot:
            results = run_dbt(["snapshot"])
            assert len(results) == 1
            assert not any(r.node.deferred for r in results)

        # copy files
        self.copy_state(project_root)


class BaseClonePossible(BaseClone):
    pass


class BaseCloneNotPossible(BaseClone):
    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "macros.sql": macros_sql,
            "my_can_clone_tables.sql": custom_can_clone_tables_macros_sql,
            "infinite_macros.sql": infinite_macros_sql,
            "get_schema_name.sql": get_schema_name_sql,
        }

    pass


class TestClonePossible(BaseClonePossible):
    def test_clone(self, project, unique_schema, other_schema):
        project.create_test_schema(other_schema)
        self.run_and_save_state(project.project_root, with_snapshot=True)

        clone_args = [
            "clone",
            "--state",
            "state",
            "--target",
            "otherschema",
        ]

        results = run_dbt(clone_args)
        # TODO: need an "adapter zone" version of this test that checks to see
        # how many of the cloned objects are "pointers" (views) versus "true clones" (tables)
        # e.g. on Postgres we expect to see 4 views
        # whereas on Snowflake we'd expect to see 3 cloned tables + 1 view
        assert [r.message for r in results] == ["CREATE VIEW"] * 1
        assert [r.message for r in results] == ["CREATE TABLE"] * 3
        schema_relations = project.adapter.list_relations(
            database=project.database, schema=other_schema
        )
        assert [r.type for r in schema_relations] == ["view"] * 1

        # objects already exist, so this is a no-op
        results = run_dbt(clone_args)
        assert [r.message for r in results] == ["No-op"] * 4

        # recreate all objects
        results = run_dbt(clone_args + ["--full-refresh"])
        assert [r.message for r in results] == ["CREATE VIEW"] * 4

        # select only models this time
        results = run_dbt(clone_args + ["--resource-type", "model"])
        assert len(results) == 2


# class TestCloneNotPossible(BaseCloneNotPossible):
#     def test_clone(self, project, unique_schema, other_schema):
#         project.create_test_schema(other_schema)
#         self.run_and_save_state(project.project_root, with_snapshot=True)

#         clone_args = [
#             "clone",
#             "--state",
#             "state",
#             "--target",
#             "otherschema",
#         ]

#         results = run_dbt(clone_args)
#         # TODO: need an "adapter zone" version of this test that checks to see
#         # how many of the cloned objects are "pointers" (views) versus "true clones" (tables)
#         # e.g. on Postgres we expect to see 4 views
#         # whereas on Snowflake we'd expect to see 3 cloned tables + 1 view
#         assert [r.message for r in results] == ["CREATE VIEW"] * 4
#         schema_relations = project.adapter.list_relations(
#             database=project.database, schema=other_schema
#         )
#         assert [r.type for r in schema_relations] == ["view"] * 4

#         # objects already exist, so this is a no-op
#         results = run_dbt(clone_args)
#         assert [r.message for r in results] == ["No-op"] * 4

#         # recreate all objects
#         results = run_dbt(clone_args + ["--full-refresh"])
#         assert [r.message for r in results] == ["CREATE VIEW"] * 4

#         # select only models this time
#         results = run_dbt(clone_args + ["--resource-type", "model"])
#         assert len(results) == 2
