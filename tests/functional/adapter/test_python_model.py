import pytest
from dbt.tests.adapter.python_model.test_python_model import BasePythonModelTests

basic_sql = """
select 1 as id union all
select 1 as id union all
select 1 as id union all
select 1 as id union all
select 1 as id union all
select 1 as id
"""
basic_python = """
def model(dbt, _):
    dbt.config(
        materialized='table',
    )
    df =  dbt.ref("my_sql_model")
    df2 = dbt.source('test_source', 'test_table')
    df = df.limit(2)
    return df
"""

second_sql = """
select * from {{ref('my_python_model')}}
"""
schema_yml = """version: 2
sources:
  - name: test_source
    loader: custom
    schema: "{{ var(env_var('DBT_TEST_SCHEMA_NAME_VARIABLE')) }}"
    quoting:
      identifier: True
    tags:
      - my_test_source_tag
    tables:
      - name: test_table
        identifier: src
"""

seeds__source_csv = """favorite_color,id,first_name,email,ip_address,updated_at
blue,1,Larry,lking0@miitbeian.gov.cn,'69.135.206.194',2008-09-12T19:08:31
blue,2,Larry,lperkins1@toplist.cz,'64.210.133.162',1978-05-09T04:15:14
"""


class TestPythonModelExasol(BasePythonModelTests):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"src.csv": seeds__source_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": schema_yml,
            "my_sql_model.sql": basic_sql,
            "my_python_model.py": basic_python,
            "second_sql_model.sql": second_sql,
        }
