import pytest
from dbt.tests.util import run_dbt, write_file
from dbt.tests.adapter.python_model.test_python_model import BasePythonModelTests

basic_sql = """
select 1 as id union all
select 1 as id union all
select 1 as id union all
select 1 as id union all
select 1 as id
"""
basic_python = """
def model(dbt, session):
    dbt.config(
        materialized='table',
    )
    df =  dbt.ref("my_sql_model")
    df = df.limit(2)
    return df
"""

class TestBasePythonModelSnowflake:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_sql_model.sql": basic_sql,
            "my_python_model.py": basic_python,
        }

    def test_singular_tests(self, project):
        # test command
        results = run_dbt(["run"])
        assert len(results) == 2


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

        

