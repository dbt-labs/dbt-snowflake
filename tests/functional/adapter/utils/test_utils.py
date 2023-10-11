from dbt.tests.adapter.utils.test_array_append import BaseArrayAppend
from dbt.tests.adapter.utils.test_array_concat import BaseArrayConcat
from dbt.tests.adapter.utils.test_array_construct import BaseArrayConstruct
from dbt.tests.adapter.utils.test_any_value import BaseAnyValue
from dbt.tests.adapter.utils.test_bool_or import BaseBoolOr
from dbt.tests.adapter.utils.test_cast_bool_to_text import BaseCastBoolToText
from dbt.tests.adapter.utils.test_concat import BaseConcat
from dbt.tests.adapter.utils.test_current_timestamp import BaseCurrentTimestampAware
from dbt.tests.adapter.utils.test_dateadd import BaseDateAdd
from dbt.tests.adapter.utils.test_datediff import BaseDateDiff
from dbt.tests.adapter.utils.test_date_spine import BaseDateSpine
from dbt.tests.adapter.utils.test_date_trunc import BaseDateTrunc
from dbt.tests.adapter.utils.test_escape_single_quotes import BaseEscapeSingleQuotesQuote
from dbt.tests.adapter.utils.test_escape_single_quotes import BaseEscapeSingleQuotesBackslash
from dbt.tests.adapter.utils.test_except import BaseExcept
from dbt.tests.adapter.utils.test_generate_series import BaseGenerateSeries
from dbt.tests.adapter.utils.test_get_intervals_between import BaseGetIntervalsBetween
from dbt.tests.adapter.utils.test_get_powers_of_two import BaseGetPowersOfTwo
from dbt.tests.adapter.utils.test_hash import BaseHash
from dbt.tests.adapter.utils.test_intersect import BaseIntersect
from dbt.tests.adapter.utils.test_last_day import BaseLastDay
from dbt.tests.adapter.utils.test_length import BaseLength
from dbt.tests.adapter.utils.test_listagg import BaseListagg
from dbt.tests.adapter.utils.test_position import BasePosition
from dbt.tests.adapter.utils.test_replace import BaseReplace
from dbt.tests.adapter.utils.test_right import BaseRight
from dbt.tests.adapter.utils.test_safe_cast import BaseSafeCast
from dbt.tests.adapter.utils.test_split_part import BaseSplitPart
from dbt.tests.adapter.utils.test_string_literal import BaseStringLiteral


class TestAnyValue(BaseAnyValue):
    pass


class TestArrayAppend(BaseArrayAppend):
    pass


class TestArrayConcat(BaseArrayConcat):
    pass


class TestArrayConstruct(BaseArrayConstruct):
    pass


class TestBoolOr(BaseBoolOr):
    pass


class TestCastBoolToText(BaseCastBoolToText):
    pass


class TestConcat(BaseConcat):
    pass


# Use either BaseCurrentTimestampAware or BaseCurrentTimestampNaive but not both
class TestCurrentTimestamp(BaseCurrentTimestampAware):
    pass


class TestDateAdd(BaseDateAdd):
    pass


class TestDateDiff(BaseDateDiff):
    pass


class TestDateSpine(BaseDateSpine):
    pass


class TestDateTrunc(BaseDateTrunc):
    pass


class TestEscapeSingleQuotesQuote(BaseEscapeSingleQuotesQuote):
    pass


class TestEscapeSingleQuotes(BaseEscapeSingleQuotesBackslash):
    pass


class TestExcept(BaseExcept):
    pass


class TestGenerateSeries(BaseGenerateSeries):
    pass


class TestGetIntervalsBeteween(BaseGetIntervalsBetween):
    pass


class TestGetPowersOfTwo(BaseGetPowersOfTwo):
    pass


class TestHash(BaseHash):
    pass


class TestIntersect(BaseIntersect):
    pass


class TestLastDay(BaseLastDay):
    pass


class TestLength(BaseLength):
    pass


class TestListagg(BaseListagg):
    pass


class TestPosition(BasePosition):
    pass


class TestReplace(BaseReplace):
    pass


class TestRight(BaseRight):
    pass


class TestSafeCast(BaseSafeCast):
    pass


class TestSplitPart(BaseSplitPart):
    pass


class TestStringLiteral(BaseStringLiteral):
    pass
