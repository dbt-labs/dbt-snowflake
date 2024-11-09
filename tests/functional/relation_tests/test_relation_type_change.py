from dataclasses import dataclass
from itertools import product
from typing import Optional

from dbt.tests.util import run_dbt
import pytest

from tests.functional.relation_tests import models
from tests.functional.utils import describe_dynamic_table, query_relation_type, update_model


@dataclass
class Model:
    model: str
    relation_type: str
    table_format: Optional[str] = "default"
    is_incremental: Optional[bool] = False

    @property
    def name(self) -> str:
        if self.is_incremental:
            name = f"{self.relation_type}_{self.table_format}_incremental"
        else:
            name = f"{self.relation_type}_{self.table_format}"
        return name

    @property
    def is_iceberg(self) -> bool:
        return self.table_format == "iceberg"

    @property
    def is_standard_table(self) -> bool:
        return self.relation_type == "table" and not self.is_incremental


@dataclass
class Scenario:
    initial: Model
    final: Model

    @property
    def name(self) -> str:
        return f"REPLACE_{self.initial.name}__WITH_{self.final.name}"

    @property
    def error_message(self) -> str:
        return f"Failed when migrating from: {self.initial.name} to: {self.final.name}"

    @property
    def uses_iceberg(self) -> bool:
        return any([self.initial.is_iceberg, self.final.is_iceberg])


relations = [
    Model(models.VIEW, "view"),
    Model(models.TABLE, "table", "default"),
    Model(models.INCREMENTAL_TABLE, "table", "default", is_incremental=True),
    Model(models.DYNAMIC_TABLE, "dynamic_table", "default"),
    Model(models.ICEBERG_TABLE, "table", "iceberg"),
    Model(models.INCREMENTAL_ICEBERG_TABLE, "table", "iceberg", is_incremental=True),
    Model(models.DYNAMIC_ICEBERG_TABLE, "dynamic_table", "iceberg"),
]
scenarios = [Scenario(*scenario) for scenario in product(relations, relations)]


def requires_full_refresh(scenario) -> bool:
    return any(
        [
            # we can only swap incremental to table and back if both are iceberg
            scenario.initial.is_incremental
            and scenario.final.is_standard_table
            and scenario.initial.table_format != scenario.final.table_format,
            scenario.initial.is_standard_table
            and scenario.final.is_incremental
            and scenario.initial.table_format != scenario.final.table_format,
            # we can't swap from an incremental to a dynamic table because the materialization does not handle this case
            scenario.initial.relation_type == "dynamic_table" and scenario.final.is_incremental,
        ]
    )


class TestRelationTypeChange:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"enable_iceberg_materializations": False}}

    @staticmethod
    def include(scenario) -> bool:
        return not scenario.uses_iceberg and not requires_full_refresh(scenario)

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            f"{scenario.name}.sql": scenario.initial.model
            for scenario in scenarios
            if self.include(scenario)
        }

    @pytest.fixture(scope="class", autouse=True)
    def setup(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])
        for scenario in scenarios:
            if self.include(scenario):
                update_model(project, scenario.name, scenario.final.model)
        # allow for dbt to fail so that we can see which scenarios pass and which scenarios fail
        try:
            run_dbt(["run"], expect_pass=False)
        except Exception:
            pass

    @pytest.mark.parametrize("scenario", scenarios, ids=[scenario.name for scenario in scenarios])
    def test_replace(self, project, scenario):
        if self.include(scenario):
            relation_type = query_relation_type(project, scenario.name)
            assert relation_type == scenario.final.relation_type, scenario.error_message
            if relation_type == "dynamic_table":
                dynamic_table = describe_dynamic_table(project, scenario.name)
                assert dynamic_table.catalog.table_format == scenario.final.table_format
        else:
            pytest.skip()


class TestRelationTypeChangeFullRefreshRequired(TestRelationTypeChange):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"enable_iceberg_materializations": False},
            "models": {"full_refresh": True},
        }

    @staticmethod
    def include(scenario) -> bool:
        return not scenario.uses_iceberg and requires_full_refresh(scenario)


class TestRelationTypeChangeIcebergOn(TestRelationTypeChange):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"enable_iceberg_materializations": True}}

    @staticmethod
    def include(scenario) -> bool:
        return scenario.uses_iceberg and not requires_full_refresh(scenario)


class TestRelationTypeChangeIcebergOnFullRefreshRequired(TestRelationTypeChange):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"enable_iceberg_materializations": True},
            "models": {"full_refresh": True},
        }

    @staticmethod
    def include(scenario) -> bool:
        return scenario.uses_iceberg and requires_full_refresh(scenario)
