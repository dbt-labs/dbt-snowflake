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
    table_format: Optional[str] = None
    incremental: Optional[bool] = None

    @property
    def name(self) -> str:
        if self.table_format:
            name = f"{self.relation_type}_{self.table_format}"
        else:
            name = f"{self.relation_type}"
        return name


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


relations = [
    Model(models.VIEW, "view"),
    Model(models.TABLE, "table", "default"),
    Model(models.DYNAMIC_TABLE, "dynamic_table", "default"),
    Model(models.DYNAMIC_ICEBERG_TABLE, "dynamic_table", "iceberg"),
    Model(models.ICEBERG_TABLE, "table", "iceberg"),
    Model(models.ICEBERG_INCREMENTAL_TABLE, "table", "iceberg", incremental=True),
]
scenarios = [Scenario(*scenario) for scenario in product(relations, relations)]


class TestRelationTypeChange:

    @staticmethod
    def include(scenario) -> bool:
        return (
            scenario.initial.table_format != "iceberg" and scenario.final.table_format != "iceberg"
        )

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
        run_dbt(["run"])

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


class TestRelationTypeChangeIcebergOn(TestRelationTypeChange):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"enable_iceberg_materializations": True}}

    @staticmethod
    def include(scenario) -> bool:
        """
        Upon adding the logic needed for seamless transitions to and from incremental models without data loss, we can coalesce these test cases.
        """
        return any(
            (
                # scenario 1: Everything that doesn't include incremental relations on Iceberg
                (
                    (
                        scenario.initial.table_format == "iceberg"
                        or scenario.final.table_format == "iceberg"
                    )
                    and not scenario.initial.incremental
                    and not scenario.final.incremental
                ),
                # scenario 2: Iceberg Incremental swaps allowed
                (
                    scenario.initial.table_format == "iceberg"
                    and scenario.final.table_format == "iceberg"
                ),
            )
        )
