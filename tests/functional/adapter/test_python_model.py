import pytest
from dbt.tests.util import run_dbt, write_file
from dbt.tests.adapter.python_model.test_python_model import BasePythonModelTests

class TestPythonModelSpark(BasePythonModelTests):
    pass

models__simple_python_model = """
import pandas

def model(dbt, session):
    dbt.config(
        materialized='table',
    )
    data = [[1,2]] * 10
    return pandas.DataFrame(data, columns=['test', 'test2'])
"""
models__simple_python_model_v2 = """
import pandas

def model(dbt, session):
    dbt.config(
        materialized='table',
    )
    data = [[1,2]] * 10
    return pandas.DataFrame(data, columns=['test1', 'test3'])
"""



class TestChangingSchemaSnowflake:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "simple_python_model.py": models__simple_python_model
            }
    def test_changing_schema(self,project):
        run_dbt(["run"])
        write_file(models__simple_python_model_v2, project.project_root + '/models', "simple_python_model.py")
        run_dbt(["run"])


m_1 = """
{{config(materialized='table')}}
select 1 as id union all
select 2 as id union all
select 3 as id union all
select 4 as id union all
select 5 as id
"""

incremental_python = """
def model(dbt, session):
    dbt.config(materialized="incremental", unique_key='id')
    df = dbt.ref("m_1")
    if dbt.is_incremental:
        # incremental runs should only apply to 
        df = df.filter(df.id >= session.sql(f"select max(id) from {dbt.this}").collect()[0][0])
    return df
"""

class TestIncrementalModelSnowflake:
    
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": { "+incremental_strategy": "delete+insert" }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "m_1.sql": m_1,
            "incremental.py": incremental_python
        }
    
    def test_incremental(self,project):
        # create m_1 and run incremental model the first time
        run_dbt(["run"])
        assert project.run_sql(f"select count(*) from {project.database}.{project.test_schema}.incremental", fetch="one")[0] == 5
        # running incremental model again will not cause any changes in the result model
        run_dbt(["run", "-s", "incremental"])
        assert project.run_sql(f"select count(*) from {project.database}.{project.test_schema}.incremental", fetch="one")[0] == 5
        # add 3 records with one supposed to be filtered out
        project.run_sql(f"insert into {project.database}.{project.test_schema}.m_1(id) values (0), (6), (7)")
        # validate that incremental model would correctly add 2 valid records to result model
        run_dbt(["run", "-s", "incremental"])
        assert project.run_sql(f"select count(*) from {project.database}.{project.test_schema}.incremental", fetch="one")[0] == 7


        

