from typing import Type

import pytest
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.snowflake.relation.models import SnowflakeDynamicTableTargetLagRelation


@pytest.mark.parametrize(
    "config_dict,exception",
    [
        # these are normal cases
        ({"duration": 1, "period": "seconds"}, DbtRuntimeError),
        ({"duration": 1, "period": "minutes"}, None),
        ({"duration": 1, "period": "hours"}, None),
        ({"duration": 1, "period": "days"}, None),
        ({"duration": 2, "period": "seconds"}, DbtRuntimeError),
        ({"duration": 2, "period": "minutes"}, None),
        ({"duration": 2, "period": "hours"}, None),
        ({"duration": 2, "period": "days"}, None),
        # are we sure that single versions of `period` are supported?
        ({"duration": 1, "period": "second"}, DbtRuntimeError),
        ({"duration": 1, "period": "minute"}, None),
        ({"duration": 1, "period": "hour"}, None),
        ({"duration": 1, "period": "day"}, None),
        # it seems like these are valid syntax in Snowflake, but they result in an actual lag of 60+ seconds
        # the database will show what the user requested, not what actually happens, so we throw an error
        # to enforce the docs and minimize unexpected behavior
        ({"duration": 0, "period": "seconds"}, DbtRuntimeError),
        ({"duration": 5, "period": "seconds"}, DbtRuntimeError),
        # this actually fails in Snowflake
        ({"duration": -1, "period": "seconds"}, DbtRuntimeError),
        # this fails due to parsing
        ({"duration": 100}, DbtRuntimeError),
        ({"period": "minutes"}, DbtRuntimeError),
        ({}, DbtRuntimeError),
    ],
)
def test_create_index(config_dict: dict, exception: Type[Exception]):
    if exception:
        with pytest.raises(exception):
            SnowflakeDynamicTableTargetLagRelation.from_dict(config_dict)
    else:
        my_target_lag = SnowflakeDynamicTableTargetLagRelation.from_dict(config_dict)
        assert my_target_lag.duration == config_dict.get("duration")
        assert my_target_lag.period == config_dict.get("period")
