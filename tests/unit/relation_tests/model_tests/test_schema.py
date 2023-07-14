from typing import Type

import pytest
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.snowflake.relation.models import SnowflakeSchemaRelation


@pytest.mark.parametrize(
    "config_dict,exception",
    [
        ({"name": "my_schema", "database": {"name": "my_database"}}, None),
        ({"name": "my_schema", "database": None}, DbtRuntimeError),
        ({"name": "my_schema"}, DbtRuntimeError),
        ({"name": "", "database": {"name": "my_database"}}, DbtRuntimeError),
        ({"wrong_name": "my_database", "database": {"name": "my_database"}}, DbtRuntimeError),
        (
            {"name": "my_schema", "database": {"name": "my_database"}, "meaning_of_life": 42},
            DbtRuntimeError,
        ),
        ({}, DbtRuntimeError),
    ],
)
def test_make_schema(config_dict: dict, exception: Type[Exception]):
    if exception:
        with pytest.raises(exception):
            SnowflakeSchemaRelation.from_dict(config_dict)
    else:
        my_schema = SnowflakeSchemaRelation.from_dict(config_dict)
        assert my_schema.name == config_dict.get("name")
        assert my_schema.database_name == config_dict.get("database").get("name")
