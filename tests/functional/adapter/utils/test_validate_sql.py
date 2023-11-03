from dbt.tests.adapter.basic.test_generic_tests import pytest
from dbt.tests.adapter.utils.test_validate_sql import BaseValidateSqlMethod


class TestExasolValidateSqlMethod(BaseValidateSqlMethod):
    @pytest.fixture(scope="class")
    def valid_sql(self) -> str:
        return "select 1 as column_name"
