import pytest

from dbt.tests.util import run_dbt, write_file
from dbt.tests.adapter.python_model.test_python_model import (
    BasePythonModelTests,
    BasePythonIncrementalTests,
)

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

models__custom_target_model = """
import pandas

def model(dbt, session):
    dbt.config(
        materialized="table",
        schema="MY_CUSTOM_SCHEMA",
        alias="_TEST_PYTHON_MODEL",
    )

    df = pandas.DataFrame({
        'City': ['Buenos Aires', 'Brasilia', 'Santiago', 'Bogota', 'Caracas'],
        'Country': ['Argentina', 'Brazil', 'Chile', 'Colombia', 'Venezuela'],
        'Latitude': [-34.58, -15.78, -33.45, 4.60, 10.48],
        'Longitude': [-58.66, -47.91, -70.66, -74.08, -66.86]
    })

    return df
"""

class TestPythonModelSnowflake(BasePythonModelTests):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models":{ "use_anonymous_sproc": True}
        }

class TestIncrementalSnowflake(BasePythonIncrementalTests):
    pass


class TestIncrementalSnowflakeQuoting(BasePythonModelTests):
    # ensure that 'dbt.ref()', 'dbt.this()', and py_write_table() all respect quoting
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"quoting": {"identifier": True}}


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
        use_anonymous_sproc = True
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


# https://github.com/dbt-labs/dbt-snowflake/issues/393 is notorious for being triggered on some
# environments but not others. As of writing this, we don't know the true root cause. This test may
# not fail on all systems with problems regarding custom schema model configurations.
class TestCustomSchemaWorks:
    @pytest.fixture(scope="class")
    def models(self):
        return {"custom_target_model.py": models__custom_target_model}

    def test_custom_target(self, project):
        results = run_dbt()
        assert results[0].node.schema == f"{project.test_schema}_MY_CUSTOM_SCHEMA"
