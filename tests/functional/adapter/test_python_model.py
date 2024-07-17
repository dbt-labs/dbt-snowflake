import pytest
import uuid
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
    pass


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
        return {"simple_python_model.py": models__simple_python_model}

    def test_changing_schema(self, project):
        run_dbt(["run"])
        write_file(
            models__simple_python_model_v2,
            project.project_root + "/models",
            "simple_python_model.py",
        )
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
        return {"simple_python_model.py": USE_IMPORT_MODEL}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"iris.csv": "1,2,3,4,setosa"}

    def test_import(self, project):
        project.run_sql("create or replace STAGE dbt_integration_test")
        project.run_sql(
            f"PUT file://{project.project_root}/seeds/iris.csv @dbt_integration_test/;"
        )
        run_dbt(["run"])


# https://github.com/dbt-labs/dbt-snowflake/issues/393 is notorious for being triggered on some
# environments but not others. As of writing this, we don't know the true root cause. This test may
# not fail on all systems with problems regarding custom schema model configurations.
class TestCustomSchemaWorks:
    @pytest.fixture(scope="class")
    def models(self):
        return {"custom_target_model.py": models__custom_target_model}

    @pytest.fixture(scope="function", autouse=True)
    def teardown_method(self, project):
        yield
        with project.adapter.connection_named("__test"):
            relation = project.adapter.Relation.create(
                database=project.database, schema=f"{project.test_schema}_MY_CUSTOM_SCHEMA"
            )
            project.adapter.drop_schema(relation)

    def test_custom_target(self, project):
        results = run_dbt()
        assert results[0].node.schema == f"{project.test_schema}_MY_CUSTOM_SCHEMA"


EXTERNAL_ACCESS_INTEGRATION_MODE = """
import pandas
import snowflake.snowpark as snowpark

def model(dbt, session: snowpark.Session):
    dbt.config(
        materialized="table",
        external_access_integrations=["test_external_access_integration"],
        packages=["httpx==0.26.0"]
    )
    import httpx
    return session.create_dataframe(
        pandas.DataFrame(
            [{"result": httpx.get(url="https://www.google.com").status_code}]
        )
    )
"""


class TestExternalAccessIntegration:
    @pytest.fixture(scope="class")
    def models(self):
        return {"external_access_integration_python_model.py": EXTERNAL_ACCESS_INTEGRATION_MODE}

    def test_external_access_integration(self, project):
        project.run_sql(
            "create or replace network rule test_network_rule type = host_port mode = egress value_list= ('www.google.com:443');"
        )
        project.run_sql(
            "create or replace external access integration test_external_access_integration allowed_network_rules = (test_network_rule) enabled = true;"
        )
        run_dbt(["run"])


TEST_RUN_ID = uuid.uuid4().hex
TEST_SECRET = f"test_secret_{TEST_RUN_ID}"
TEST_NETWORK_RULE = f"test_network_rule_{TEST_RUN_ID}"
TEST_EXTERNAL_ACCESS_INTEGRATION = f"test_external_access_integration_{TEST_RUN_ID}"
SECRETS_MODE = f"""
import pandas
import snowflake.snowpark as snowpark

def model(dbt, session: snowpark.Session):
    dbt.config(
        materialized="table",
        secrets={{"secret_variable_name": "{TEST_SECRET}"}},
        external_access_integrations=["{TEST_EXTERNAL_ACCESS_INTEGRATION}"],
    )
    import _snowflake
    return session.create_dataframe(
        pandas.DataFrame(
            [{{"secret_value": _snowflake.get_generic_secret_string('secret_variable_name')}}]
        )
    )
"""


class TestSecrets:
    @pytest.fixture(scope="class")
    def models(self):
        return {"secret_python_model.py": SECRETS_MODE}

    @pytest.fixture(scope="class")
    def profiles_config_update(self):
        return {"retry_all": True, "connect_retries": 3}

    def test_secrets(self, project):
        project.run_sql(
            f"create or replace secret {TEST_SECRET} type = generic_string secret_string='secret value';"
        )

        project.run_sql(
            f"create or replace network rule {TEST_NETWORK_RULE} type = host_port mode = egress value_list= ('www.google.com:443');"
        )

        project.run_sql(
            f"create or replace external access integration {TEST_EXTERNAL_ACCESS_INTEGRATION} "
            f"allowed_network_rules = ({TEST_NETWORK_RULE}) "
            f"allowed_authentication_secrets = ({TEST_SECRET}) enabled = true;"
        )

        run_dbt(["run"])

        project.run_sql(f"drop secret if exists {TEST_SECRET};")
        project.run_sql(f"drop network rule if exists {TEST_NETWORK_RULE};")
        project.run_sql(
            f"drop external access integration if exists {TEST_EXTERNAL_ACCESS_INTEGRATION};"
        )
