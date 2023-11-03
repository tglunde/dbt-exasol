exasol_model_schema_yml = """
version: 2
models:
  - name: my_model
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: integer
        description: hello
        constraints:
          - type: not_null
          - type: check
            expression: (id > 0)
        tests:
          - unique
      - name: color
        data_type: char(50)
      - name: date_day
        data_type: char(50)
  - name: my_model_error
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: integer
        description: hello
        constraints:
          - type: not_null
          - type: check
            expression: (id > 0)
        tests:
          - unique
      - name: color
        data_type: char(50)
      - name: date_day
        data_type: char(50)
  - name: my_model_wrong_order
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: integer
        description: hello
        constraints:
          - type: not_null
          - type: check
            expression: (id > 0)
        tests:
          - unique
      - name: color
        data_type: char(50)
      - name: date_day
        data_type: char(50)
  - name: my_model_wrong_name
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: integer
        description: hello
        constraints:
          - type: not_null
          - type: check
            expression: (id > 0)
        tests:
          - unique
      - name: color
        data_type: char(50)
      - name: date_day
        data_type: char(50)
"""

exasol_constrained_model_schema_yml = """
version: 2
models:
  - name: my_model
    config:
      contract:
        enforced: true
    constraints:
      - type: check
        expression: (id > 0)
      - type: primary_key
        columns: [ id ]
      - type: unique
        columns: [ color, date_day ]
        name: strange_uniqueness_requirement
    columns:
      - name: id
        data_type: integer
        description: hello
        constraints:
          - type: not_null
        tests:
          - unique
      - name: color
        data_type: char(50)
      - name: date_day
        data_type: char(50)
"""

exasol_quoted_column_schema_yml = """
version: 2
models:
  - name: my_model
    config:
      contract:
        enforced: true
      materialized: table
    constraints:
      - type: check
        # this one is the on the user
        expression: ("from" = 'blue')
        columns: [ '"from"' ]
    columns:
      - name: id
        data_type: integer
        description: hello
        constraints:
          - type: not_null
        tests:
          - unique
      - name: from  # reserved word
        quote: true
        data_type: char(50)
        constraints:
          - type: not_null
      - name: date_day
        data_type: char(50)
"""

my_model_view_wrong_order_sql = """
{{
  config(
    materialized = "view"
  )
}}

select
  cast('blue' as char(50)) as color,
  cast(1 as integer) as id,
  cast('2019-01-01' as char(50)) as date_day
"""

my_model_view_wrong_name_sql = """
{{
  config(
    materialized = "view"
  )
}}

select
  cast('blue' as char(50)) as color,
  cast(1 as integer) as error,
  cast('2019-01-01' as char(50)) as date_day
"""

my_model_view_wrong_data_type_sql = """
{{
  config(
    materialized = "view"
  )
}}

select
  cast('1' as char(50)) as wrong_data_type_column_name
)
"""

exasol_expected_sql = """
create or replace table <model_identifier> ( 
    id integer not null, 
    color char(50), 
    date_day char(50) 
    ) ; 
    |separatemeplease| 
    insert into <model_identifier> 
    select id, color, date_day from ( select 'blue' as color, 1 as id, '2019-01-01' as date_day ) as model_subq
"""

exasol_model_contract_sql_header_sql = """
{{
  config(
    materialized = "table"
  )
}}

{% call set_sql_header(config) %}
alter session set TIME_ZONE = 'Asia/Kolkata';
{%- endcall %}
select session_parameter(current_session, 'TIME_ZONE') as column_name
"""

exasol_model_incremental_contract_sql_header = """
{{
  config(
    materialized = "incremental",
    on_schema_change="append_new_columns"
  )
}}

{% call set_sql_header(config) %}
alter session set TIME_ZONE = 'Asia/Kolkata';
{%- endcall %}
select session_parameter(current_session, 'TIME_ZONE') as column_name
"""

exasol_model_contract_header_schema_yml = """
version: 2
models:
  - name: my_model_contract_sql_header
    config:
      contract:
        enforced: true
    columns:
      - name: column_name
        data_type: varchar(2000000)
"""
