import os

import pytest
from dbt.exceptions import CompilationException
from dbt.tests.adapter.utils.base_utils import BaseUtils

# from dbt.tests.adapter.utils.test_array_append import BaseArrayAppend
# from dbt.tests.adapter.utils.test_array_concat import BaseArrayConcat
# from dbt.tests.adapter.utils.test_array_construct import BaseArrayConstruct
from dbt.tests.adapter.utils.test_any_value import BaseAnyValue
from dbt.tests.adapter.utils.test_bool_or import BaseBoolOr
from dbt.tests.adapter.utils.test_cast_bool_to_text import BaseCastBoolToText
from dbt.tests.adapter.utils.test_concat import BaseConcat
from dbt.tests.adapter.utils.test_date_trunc import BaseDateTrunc

# from dbt.tests.adapter.utils.test_current_timestamp import BaseCurrentTimestampAware
from dbt.tests.adapter.utils.test_dateadd import BaseDateAdd
from dbt.tests.adapter.utils.test_datediff import BaseDateDiff
from dbt.tests.adapter.utils.test_escape_single_quotes import (
    BaseEscapeSingleQuotesBackslash,
    BaseEscapeSingleQuotesQuote,
)
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
from dbt.tests.util import run_dbt
from utils_fixtures import *


class TestAnyValue(BaseAnyValue):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_any_value.yml": exasol__models__test_any_value_yml,
            "test_any_value.sql": self.interpolate_macro_namespace(
                exasol__models__test_any_value_sql, "any_value"
            ),
        }


# class TestArrayAppend(BaseArrayAppend):
#    pass


# class TestArrayConcat(BaseArrayConcat):
#    pass


# class TestArrayConstruct(BaseArrayConstruct):
#    pass


class TestBoolOr(BaseBoolOr):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "data_bool_or.csv": exasol__seeds__data_bool_or_csv,
            "data_bool_or_expected.csv": exasol__seeds__data_bool_or_expected_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_bool_or.yml": exasol__models__test_bool_or_yml,
            "test_bool_or.sql": self.interpolate_macro_namespace(
                exasol__models__test_bool_or_sql, "bool_or"
            ),
        }


class TestCastBoolToText(BaseCastBoolToText):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_cast_bool_to_text.yml": exasol__models__test_cast_bool_to_text_yml,
            "test_cast_bool_to_text.sql": self.interpolate_macro_namespace(
                exasol__models__test_cast_bool_to_text_sql, "cast_bool_to_text"
            ),
        }


class TestConcat(BaseConcat):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_concat.csv": exasol__seeds__data_concat_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_concat.yml": exasol__models__test_concat_yml,
            "test_concat.sql": self.interpolate_macro_namespace(
                exasol__models__test_concat_sql, "concat"
            ),
        }


# Use either BaseCurrentTimestampAware or BaseCurrentTimestampNaive but not both
# class TestCurrentTimestamp(BaseCurrentTimestampAware):
#    pass


class TestDateAdd(BaseDateAdd):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "test",
            # this is only needed for BigQuery, right?
            # no harm having it here until/unless there's an adapter that doesn't support the 'timestamp' type
            "seeds": {
                "test": {
                    "data_dateadd": {
                        "+column_types": {
                            "from_time": "timestamp",
                            "result": "timestamp",
                        },
                    },
                },
            },
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_dateadd.csv": exasol__seeds__data_dateadd_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_dateadd.yml": exasol__models__test_dateadd_yml,
            "test_dateadd.sql": self.interpolate_macro_namespace(
                exasol__models__test_dateadd_sql, "dateadd"
            ),
        }


class TestDateDiff(BaseDateDiff):
    @pytest.fixture(scope="class")
    def dbt_profile_target(self):
        return {
            "type": "exasol",
            "threads": 1,
            "dsn": os.getenv("DBT_DSN", "localhost:8563"),
            "user": os.getenv("DBT_USER", "sys"),
            "pass": os.getenv("DBT_PASS", "exasol"),
            "dbname": "DB",
            "timestamp_format": "YYYY-MM-DD HH:MI:SS.FF6",
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_datediff.csv": exasol__seeds__data_datediff_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_datediff.yml": exasol__models__test_datediff_yml,
            "test_datediff.sql": self.interpolate_macro_namespace(
                exasol__models__test_datediff_sql, "datediff"
            ),
        }


class TestDateTrunc(BaseDateTrunc):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_date_trunc.csv": exasol__seeds__data_date_trunc_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_date_trunc.yml": exasol__models__test_date_trunc_yml,
            "test_date_trunc.sql": self.interpolate_macro_namespace(
                exasol__models__test_date_trunc_sql, "date_trunc"
            ),
        }


class TestEscapeSingleQuotes(BaseEscapeSingleQuotesBackslash):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_escape_single_quotes.yml": exasol__models__test_escape_single_quotes_yml,
            "test_escape_single_quotes.sql": self.interpolate_macro_namespace(
                exasol__models__test_escape_single_quotes_quote_sql,
                "escape_single_quotes",
            ),
        }


class BaseEscapeSingleQuotesBackslash(BaseUtils):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_escape_single_quotes.yml": exasol__models__test_escape_single_quotes_yml,
            "test_escape_single_quotes.sql": self.interpolate_macro_namespace(
                exasol__models__test_escape_single_quotes_backslash_sql,
                "escape_single_quotes",
            ),
        }


class TestExcept(BaseExcept):
    pass


class TestHash(BaseHash):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_hash.csv": exasol__seeds__data_hash_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_hash.yml": exasol__models__test_hash_yml,
            "test_hash.sql": self.interpolate_macro_namespace(
                exasol__models__test_hash_sql, "hash"
            ),
        }


class TestIntersect(BaseIntersect):
    pass


class TestLastDay(BaseLastDay):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_last_day.csv": exasol__seeds__data_last_day_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_last_day.yml": exasol__models__test_last_day_yml,
            "test_last_day.sql": self.interpolate_macro_namespace(
                exasol__models__test_last_day_sql, "last_day"
            ),
        }


class TestLength(BaseLength):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_length.csv": exasol__seeds__data_length_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_length.yml": exasol__models__test_length_yml,
            "test_length.sql": self.interpolate_macro_namespace(
                exasol__models__test_length_sql, "length"
            ),
        }


class TestListagg(BaseListagg):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "data_listagg.csv": exasol__seeds__data_listagg_csv,
            "data_listagg_output.csv": exasol__seeds__data_listagg_output_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_listagg.yml": exasol__models__test_listagg_yml,
            "test_listagg.sql": self.interpolate_macro_namespace(
                exasol__models__test_listagg_sql, "listagg"
            ),
        }

    def test_build_assert_equal(self, project):
        with pytest.raises(CompilationException) as exc_info:
            run_dbt(["build"], expect_pass=False)
        assert exc_info.value.msg == "`limit_num` parameter is not supported on Exasol!"


class TestPosition(BasePosition):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_position.csv": exasol__seeds__data_position_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_position.yml": exasol__models__test_position_yml,
            "test_position.sql": self.interpolate_macro_namespace(
                exasol__models__test_position_sql, "position"
            ),
        }


class TestReplace(BaseReplace):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_replace.csv": exasol__seeds__data_replace_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_replace.yml": exasol__models__test_replace_yml,
            "test_replace.sql": self.interpolate_macro_namespace(
                exasol__models__test_replace_sql, "replace"
            ),
        }


class TestRight(BaseRight):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_right.csv": exasol__seeds__data_right_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_right.yml": exasol__models__test_right_yml,
            "test_right.sql": self.interpolate_macro_namespace(
                exasol__models__test_right_sql, "right"
            ),
        }


class TestSafeCast(BaseSafeCast):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_safe_cast.csv": exasol__seeds__data_safe_cast_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_safe_cast.yml": exasol__models__test_safe_cast_yml,
            "test_safe_cast.sql": self.interpolate_macro_namespace(
                self.interpolate_macro_namespace(
                    exasol__models__test_safe_cast_sql, "safe_cast"
                ),
                "type_string",
            ),
        }


class TestSplitPart(BaseSplitPart):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_split_part.csv": exasol__seeds__data_split_part_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_split_part.yml": exasol__models__test_split_part_yml,
            "test_split_part.sql": self.interpolate_macro_namespace(
                exasol__models__test_split_part_sql, "split_part"
            ),
        }

    def test_build_assert_equal(self, project):
        with pytest.raises(CompilationException) as exc_info:
            run_dbt(["build"], expect_pass=False)
        assert exc_info.value.msg == "Unsupported on Exasol! Sorry..."


class TestStringLiteral(BaseStringLiteral):
    pass
