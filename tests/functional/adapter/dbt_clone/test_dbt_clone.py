import pytest
import shutil
import os
from copy import deepcopy
from dbt.tests.util import run_dbt
from dbt.tests.adapter.dbt_clone.test_dbt_clone import BaseClonePossible


class TestSnowflakeClonePossible(BaseClonePossible):
    @pytest.fixture(autouse=True)
    def clean_up(self, project):
        yield
        with project.adapter.connection_named("__test"):
            relation = project.adapter.Relation.create(
                database=project.database, schema=f"{project.test_schema}_SEEDS"
            )
            project.adapter.drop_schema(relation)

            relation = project.adapter.Relation.create(
                database=project.database, schema=project.test_schema
            )
            project.adapter.drop_schema(relation)

    pass


table_model_1_sql = """
    {{ config(
        materialized='table',
        transient=true,
    ) }}

    select 1 as fun
    """


class TestSnowflakeCloneTrainsentTable:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table_model.sql": table_model_1_sql,
        }

    @pytest.fixture(scope="class")
    def other_schema(self, unique_schema):
        return unique_schema + "_other"

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
        results = run_dbt(["run"])
        assert len(results) == 1

        self.copy_state(project_root)

    def test_can_clone_transient_table(self, project, other_schema):
        project.create_test_schema(other_schema)
        self.run_and_save_state(project.project_root)

        clone_args = [
            "clone",
            "--state",
            "state",
            "--target",
            "otherschema",
        ]

        results = run_dbt(clone_args)
        assert len(results) == 1
