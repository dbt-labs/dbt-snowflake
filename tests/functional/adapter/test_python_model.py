import pytest
from dbt.tests.util import run_dbt, write_file, run_sql_with_adapter
from dbt.tests.adapter.python_model.test_python_model import BasePythonModelTests, BasePythonIncrementalTests


class TestPythonModelSnowflake(BasePythonModelTests):
    pass


class TestIncrementalSnowflake(BasePythonIncrementalTests):
    pass


class TestIncrementalSnowflakeQuoting(BasePythonModelTests):
    # ensure that 'dbt.ref()', 'dbt.this()', and py_write_table() all respect quoting
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"quoting": {"identifier": True}}


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

USE_IMPORT_MODEL = """
import sys
from snowflake.snowpark.types import StructType, FloatType, StringType, StructField

def model( dbt, session):

    dbt.config(
        materialized='table',
        imports = ['@dbt_integration_test/iris.csv'],
        use_anonymous_sproc = False
    )
    schema_for_data_file = StructType([
        StructField("length1", FloatType()),
        StructField("width1", FloatType()),
        StructField("length2", FloatType()),
        StructField("width2", FloatType()),
        StructField("variety", StringType()),
    ])
    df = session.read.schema(schema_for_data_file).option("field_delimiter", ",").schema(schema_for_data_file).csv("@dbt_integration_test/iris.csv")
    return df
"""

class TestImportSnowflake:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "simple_python_model.py": USE_IMPORT_MODEL
        }
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "iris.csv": "1,2,3,4,setosa"
        }
    def test_import(self,project):
        project.run_sql("create or replace STAGE dbt_integration_test")
        project.run_sql(f"PUT file://{project.project_root}/seeds/iris.csv @dbt_integration_test/;")
        run_dbt(["run"])
