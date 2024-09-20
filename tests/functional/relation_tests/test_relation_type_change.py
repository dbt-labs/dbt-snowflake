from dataclasses import dataclass
from itertools import product

from dbt.tests.util import run_dbt
import pytest

from tests.functional.relation_tests import models
from tests.functional.utils import query_relation_type, update_model


@dataclass
class Model:
    model: str
    relation_type: str

    @property
    def name(self) -> str:
        return f"{self.relation_type}"


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
    Model(models.TABLE, "table"),
    Model(models.DYNAMIC_TABLE, "dynamic_table"),
]
scenarios = [Scenario(*scenario) for scenario in product(relations, relations)]


class TestRelationTypeChange:

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {f"{scenario.name}.sql": scenario.initial.model for scenario in scenarios}

    @pytest.fixture(scope="class", autouse=True)
    def setup(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])
        for scenario in scenarios:
            update_model(project, scenario.name, scenario.final.model)
        run_dbt(["run"])

    @pytest.mark.parametrize("scenario", scenarios, ids=[scenario.name for scenario in scenarios])
    def test_replace(self, project, scenario):
        relation_type = query_relation_type(project, scenario.name)
        assert relation_type == scenario.final.relation_type, scenario.error_message
