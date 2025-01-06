from dbt.contracts.results import CatalogArtifact
from dbt.tests.util import run_dbt
import pytest

from tests.functional.adapter.catalog_tests import files


class TestCatalogRelationTypes:
    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"my_seed.csv": files.MY_SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "my_table.sql": files.MY_TABLE,
            "my_view.sql": files.MY_VIEW,
            "my_dynamic_table.sql": files.MY_DYNAMIC_TABLE,
        }

    @pytest.fixture(scope="class", autouse=True)
    def docs(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])
        yield run_dbt(["docs", "generate"])

    @pytest.mark.parametrize(
        "node_name,relation_type",
        [
            ("seed.test.my_seed", "BASE TABLE"),
            ("model.test.my_table", "BASE TABLE"),
            ("model.test.my_view", "VIEW"),
            ("model.test.my_dynamic_table", "DYNAMIC TABLE"),
        ],
    )
    def test_relation_types_populate_correctly(
        self, docs: CatalogArtifact, node_name: str, relation_type: str
    ):
        """
        This test addresses: https://github.com/dbt-labs/dbt-snowflake/issues/817
        """
        assert node_name in docs.nodes
        node = docs.nodes[node_name]
        assert node.metadata.type == relation_type
