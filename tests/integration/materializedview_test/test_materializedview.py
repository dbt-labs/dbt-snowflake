import pytest

from dbt.tests.util import run_dbt, run_sql_with_adapter, check_relations_equal, relation_from_name
from dbt.tests.adapter.basic.files import (
    seeds_base_csv,
    seeds_added_csv,
    schema_base_yml
)

materializedview_sql = """
{{ config(materialized="materializedview",
  cluster_by=var('cluster', [])
  )}}
select * from {{ source('raw', 'seed') }}
""".strip()

class BaseMaterializedView:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "materializedview"}

    @pytest.fixture(scope="class")
    def models(self):
        return {"materializedview.sql": materializedview_sql, "schema.yml": schema_base_yml}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"base.csv": seeds_base_csv, "added.csv": seeds_added_csv}

    def test_materializedview(self, project):
        # seed command
        results = run_dbt(["seed"])
        assert len(results) == 2

        # base table rowcount
        relation = relation_from_name(project.adapter, "base")
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
        assert result[0] == 10

        # added table rowcount
        relation = relation_from_name(project.adapter, "added")
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
        assert result[0] == 20

        # run command
        # the "seed_name" var changes the seed identifier in the schema file
        results = run_dbt(["run", "--vars", "seed_name: base"])
        assert len(results) == 1

        # check relations equal
        check_relations_equal(project.adapter, ["base", "materializedview"])

        # change seed_name var
        # the "seed_name" var changes the seed identifier in the schema file
        results = run_dbt(["run", "--vars", "seed_name: added"])
        assert len(results) == 1

        # check relations equal
        check_relations_equal(project.adapter, ["added", "materializedview"])

        # get catalog from docs generate
        catalog = run_dbt(["docs", "generate"])
        assert len(catalog.nodes) == 3
        assert len(catalog.sources) == 1

    def test_materializedview_cluster(self, project):
        # seed command
        results = run_dbt(["seed"])
        assert len(results) == 2

        cluster_combinations = {"['id']": 'LINEAR(id)', 
                                "['id','name']": 'LINEAR(id,name)', 
                                "[]": None, 
                                None: None
                                }
        for cluster, expected_result in cluster_combinations.items():
            # run command
            if cluster is not None:
                results = run_dbt(["run", "--vars", f"{{seed_name: base, cluster: {cluster} }}"])
            else:
                results = run_dbt(["run", "--vars", "{seed_name: base}"])
            assert len(results) == 1
    
            clustering_key = run_sql_with_adapter(project.adapter, sql=f'''select clustering_key
                                                                from {project.adapter.config.credentials.database}.information_schema.tables
                                                                where table_schema = upper('{project.adapter.config.credentials.schema}')
                                                                and table_name = 'MATERIALIZEDVIEW'
                                                                ''', fetch='one')
            assert clustering_key[0] == expected_result

    def test_invalid_materializedview_cluster(self, project):
         # seed command
        run_dbt(["seed"])
        # run command with invalid cluster key
        output = run_dbt(["run", "--vars", "{seed_name: base, cluster: fake_column_name}"], expect_pass=False)
        assert output.results[0].status == 'error'

class TestMaterializedview(BaseMaterializedView):
    pass