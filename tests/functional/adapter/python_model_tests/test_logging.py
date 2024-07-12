from dbt.tests.util import run_dbt
import pytest

from tests.functional.adapter.python_model_tests._files import MODEL__LOGGING

EVENT_TABLE_SQL = """
SELECT
    RECORD['severity_text']::STRING AS log_level,
    VALUE::STRING AS message,
    RESOURCE_ATTRIBUTES['snow.query.id']::STRING AS query_id
FROM
    DXRX_OPERATIONS.LOGGING.EVENTS
WHERE
    SCOPE['name']::STRING = 'dbt_logger'
ORDER BY
    'TIMESTAMP' DESC
;
"""


class TestPythonModelLogging:
    @pytest.fixture(scope="class")
    def models(self):
        return {"logging_model.py": MODEL__LOGGING}

    def test_logging(self, project):
        run_dbt(["run"])
