"""
The first time using an account for testing, you should run this:

```
CREATE OR REPLACE SECURITY INTEGRATION DBT_INTEGRATION_TEST_OAUTH
  TYPE = OAUTH
  ENABLED = TRUE
  OAUTH_CLIENT = CUSTOM
  OAUTH_CLIENT_TYPE = 'CONFIDENTIAL'
  OAUTH_REDIRECT_URI = 'http://localhost:8080'
  oauth_issue_refresh_tokens = true
  OAUTH_ALLOW_NON_TLS_REDIRECT_URI = true
  BLOCKED_ROLES_LIST = <everything but your integration test role goes here: ('ACCOUNTADMIN', 'SECURITYADMIN'), or ignore it if you don't care>
  oauth_refresh_token_validity = 7776000;
```


Every month (or any amount <90 days):

Run `select SYSTEM$SHOW_OAUTH_CLIENT_SECRETS('DBT_INTEGRATION_TEST_OAUTH');`

The only row/column of output should be a json blob, it goes (within single
quotes!) as the second argument to the server script:

python scripts/werkzeug-refresh-token.py ${acount_name} '${json_blob}'

Open http://localhost:8080

Log in as the test user, get a response page with some environment variables.
Update CI providers and test.env with the new values (If you kept the security
integration the same, just the refresh token changed)
"""

import os
from dbt.tests.util import check_relations_equal, run_dbt
import pytest


_MODELS__MODEL_1_SQL = """
select 1 as id
"""


_MODELS__MODEL_2_SQL = """
select 2 as id
"""


_MODELS__MODEL_3_SQL = """
select * from {{ ref('model_1') }}
union all
select * from {{ ref('model_2') }}
"""


_MODELS__MODEL_4_SQL = """
select 1 as id
union all
select 2 as id
"""


class TestSnowflakeOauth:
    @pytest.fixture(scope="class", autouse=True)
    def dbt_profile_target(self):
        return {
            "type": "snowflake",
            "threads": 4,
            "account": os.getenv("SNOWFLAKE_TEST_ACCOUNT"),
            "user": os.getenv("SNOWFLAKE_TEST_USER"),
            "oauth_client_id": os.getenv("SNOWFLAKE_TEST_OAUTH_CLIENT_ID"),
            "oauth_client_secret": os.getenv("SNOWFLAKE_TEST_OAUTH_CLIENT_SECRET"),
            "token": os.getenv("SNOWFLAKE_TEST_OAUTH_REFRESH_TOKEN"),
            "database": os.getenv("SNOWFLAKE_TEST_DATABASE"),
            "warehouse": os.getenv("SNOWFLAKE_TEST_WAREHOUSE"),
            "authenticator": "oauth",
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_1.sql": _MODELS__MODEL_1_SQL,
            "model_2.sql": _MODELS__MODEL_2_SQL,
            "model_3.sql": _MODELS__MODEL_3_SQL,
            "model_4.sql": _MODELS__MODEL_4_SQL,
        }

    def test_snowflake_basic(self, project):
        run_dbt()
        check_relations_equal(project.adapter, ["MODEL_3", "MODEL_4"])
