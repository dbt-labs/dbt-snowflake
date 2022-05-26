import pytest
from dbt.tests.adapter.utils.base_utils import BaseUtils
from dbt.tests.adapter.utils.test_any_value import BaseAnyValue
from dbt.tests.adapter.utils.test_bool_or import BaseBoolOr
from dbt.tests.adapter.utils.test_cast_bool_to_text import BaseCastBoolToText
from dbt.tests.adapter.utils.test_concat import BaseConcat
from dbt.tests.adapter.utils.test_current_timestamp_in_utc import BaseCurrentTimestampInUtc
from dbt.tests.adapter.utils.test_current_timestamp import BaseCurrentTimestamp
from dbt.tests.adapter.utils.test_dateadd import BaseDateAdd
from dbt.tests.adapter.utils.test_datediff import BaseDateDiff
from dbt.tests.adapter.utils.test_date_trunc import BaseDateTrunc
from dbt.tests.adapter.utils.test_escape_single_quotes import BaseEscapeSingleQuotesQuote
from dbt.tests.adapter.utils.test_escape_single_quotes import BaseEscapeSingleQuotesBackslash
from dbt.tests.adapter.utils.test_except import BaseExcept
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
from dbt.tests.adapter.utils.test_type_bigint import BaseTypeBigint
from dbt.tests.adapter.utils.test_type_boolean import BaseTypeBoolean
from dbt.tests.adapter.utils.test_type_float import BaseTypeFloat
from dbt.tests.adapter.utils.test_type_int import BaseTypeInt
from dbt.tests.adapter.utils.test_type_numeric import BaseTypeNumeric
from dbt.tests.adapter.utils.test_type_string import BaseTypeString
from dbt.tests.adapter.utils.test_type_timestamp import BaseTypeTimestamp


class TestAnyValue(BaseAnyValue):
    pass


class TestBoolOr(BaseBoolOr):
    pass


class TestCastBoolToText(BaseCastBoolToText):
    pass


class TestConcat(BaseConcat):
    pass


class TestCurrentTimestampInUtc(BaseCurrentTimestampInUtc):
    pass


class TestCurrentTimestamp(BaseCurrentTimestamp):
    pass


class TestDateAdd(BaseDateAdd):
    pass


class TestDateDiff(BaseDateDiff):
    pass


class TestDateTrunc(BaseDateTrunc):
    pass


@pytest.mark.only_profile("postgres")
class TestEscapeSingleQuotesPostgres(BaseEscapeSingleQuotesQuote):
    pass


@pytest.mark.only_profile("redshift")
class TestEscapeSingleQuotesRedshift(BaseEscapeSingleQuotesQuote):
    pass


@pytest.mark.only_profile("snowflake")
class TestEscapeSingleQuotesSnowflake(BaseEscapeSingleQuotesBackslash):
    pass


@pytest.mark.only_profile("bigquery")
class TestEscapeSingleQuotesBigQuery(BaseEscapeSingleQuotesBackslash):
    pass


class TestExcept(BaseExcept):
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


@pytest.mark.skip(reason="TODO - implement this test")
class TestTypeBigint(BaseTypeBigint):
    pass


@pytest.mark.skip(reason="TODO - implement this test")
class TestTypeBoolean(BaseTypeBoolean):
    pass


@pytest.mark.skip(reason="TODO - implement this test")
class TestTypeFloat(BaseTypeFloat):
    pass


@pytest.mark.skip(reason="TODO - implement this test")
class TestTypeInt(BaseTypeInt):
    pass


@pytest.mark.skip(reason="TODO - implement this test")
class TestTypeNumeric(BaseTypeNumeric):
    pass


@pytest.mark.skip(reason="TODO - implement this test")
class TestTypeString(BaseTypeString):
    pass


@pytest.mark.skip(reason="TODO - implement this test")
class TestTypeTimestamp(BaseTypeTimestamp):
    pass
