import pytest
from dbt.tests.adapter.constraints.fixtures import (
    my_incremental_model_sql,
    my_model_incremental_wrong_name_sql,
    my_model_incremental_wrong_order_sql,
    my_model_sql,
    my_model_wrong_name_sql,
    my_model_wrong_order_sql,
)
from dbt.tests.adapter.constraints.test_constraints import (
    BaseConstraintsRollback,
    BaseConstraintsRuntimeDdlEnforcement,
    BaseIncrementalConstraintsColumnsEqual,
    BaseIncrementalConstraintsRollback,
    BaseIncrementalConstraintsRuntimeDdlEnforcement,
    BaseModelConstraintsRuntimeEnforcement,
    BaseTableConstraintsColumnsEqual,
    BaseViewConstraintsColumnsEqual,
)

from tests.functional.adapter.constraints.fixtures import (
    exasol_constrained_model_schema_yml,
    exasol_model_schema_yml,
    my_model_view_wrong_order_sql,
    my_model_view_wrong_name_sql,
    exasol_expected_sql
)

class ExasolColumnEqualSetup:
    @pytest.fixture
    def string_type(self):
        return "CHAR(50)"
    
    @pytest.fixture
    def int_type(self):
        return "DECIMAL(10,2)"

    @pytest.fixture
    def data_types(self, schema_int_type, int_type, string_type):
        # sql_column_value, schema_data_type, error_data_type
        return [
            ["1", schema_int_type, int_type],
            ["'1'", string_type, string_type],
            ["cast('2019-01-01' as date)", "date", "DATE"],
            ["true", "boolean", "BOOLEAN"],
            ["cast('2013-11-03T00:00:00.000000' as TIMESTAMP)", "timestamp(6)", "TIMESTAMP"],
            # [
            #     "cast('2013-11-03T00:00:00.000000' as TIMESTAMP WITH LOCAL TIME ZONE)",
            #     "timestamp(6)",
            #     "TIMESTAMP",
            # ],
            ["cast('1' as DECIMAL(10,2))", "DECIMAL", "DECIMAL"],
        ]


class TestExasolTableConstraintsColumnsEqual(
    ExasolColumnEqualSetup, BaseTableConstraintsColumnsEqual
):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_wrong_order.sql": my_model_wrong_order_sql,
            "my_model_wrong_name.sql": my_model_wrong_name_sql,
            "constraints_schema.yml": exasol_model_schema_yml,
        }


class TestExasolViewConstraintsColumnsEqual(ExasolColumnEqualSetup, BaseViewConstraintsColumnsEqual):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_wrong_order.sql": my_model_view_wrong_order_sql,
            "my_model_wrong_name.sql": my_model_view_wrong_name_sql,
            "constraints_schema.yml": exasol_model_schema_yml,
        }

class TestExasolIncrementalConstraintsColumnsEqual(
    ExasolColumnEqualSetup, BaseIncrementalConstraintsColumnsEqual
):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_wrong_order.sql": my_model_incremental_wrong_order_sql,
            "my_model_wrong_name.sql": my_model_incremental_wrong_name_sql,
            "constraints_schema.yml": exasol_model_schema_yml,
        }
    # TODO: if necessary, this test can be suppressed
    def test__constraints_wrong_column_data_types(self, project, string_type, int_type, schema_string_type, schema_int_type, data_types):
        pass


class TestExasolTableConstraintsRuntimeDdlEnforcement(BaseConstraintsRuntimeDdlEnforcement):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_wrong_order_sql,
            "constraints_schema.yml": exasol_model_schema_yml,
        }

    @pytest.fixture(scope="class")
    def expected_sql(self):
        return exasol_expected_sql



# class TestExasolTableConstraintsRollback(BaseConstraintsRollback):
#     @pytest.fixture(scope="class")
#     def models(self):
#         return {
#             "my_model.sql": my_model_sql,
#             "constraints_schema.yml": exasol_model_schema_yml,
#         }

#     @pytest.fixture(scope="class")
#     def expected_error_messages(self):
#         return ["constraint violation - not null"]
    
#     # TODO: error messages work differently in Exasol,
#     # which is why it cannot be mapped
#     # Can I override a specific subfunction of the BaseConstraintsRollback class?
#     # Doesn't seem to work
#     @pytest.fixture(scope="class")
#     def assert_expected_error_messages(self, error_message, expected_error_messages):
#         assert expected_error_messages[0] in error_message


class TestExasolIncrementalConstraintsRuntimeDdlEnforcement(
    BaseIncrementalConstraintsRuntimeDdlEnforcement
):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_incremental_wrong_order_sql,
            "constraints_schema.yml": exasol_model_schema_yml,
        }

    @pytest.fixture(scope="class")
    def expected_sql(self):
        return exasol_expected_sql


# class TestExasolIncrementalConstraintsRollback(BaseIncrementalConstraintsRollback):
#     @pytest.fixture(scope="class")
#     def models(self):
#         return {
#             "my_model.sql": my_incremental_model_sql,
#             "constraints_schema.yml": exasol_model_schema_yml,
#         }

#     @pytest.fixture(scope="class")
#     def expected_error_messages(self):
#         return ["NULL value not allowed for NOT NULL column: id"]



class TestExasolModelConstraintsRuntimeEnforcement(BaseModelConstraintsRuntimeEnforcement):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "constraints_schema.yml": exasol_constrained_model_schema_yml,
        }

    @pytest.fixture(scope="class")
    def expected_sql(self):
        return """
create or replace table <model_identifier> ( 
    id decimal(10,2) not null, 
    color char(50), 
    date_day char(50), 
    primary key (id) ) ; 
    |separatemeplease| 
    insert into <model_identifier> 
    select 
    id, 
    color, 
    date_day 
    from ( 
        select 1 as id, 
        'blue' as color, 
        '2019-01-01' as date_day ) as model_subq
"""