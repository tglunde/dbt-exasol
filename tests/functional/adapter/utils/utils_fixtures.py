# any_value

exasol__models__test_any_value_sql = """
with testdata as (

    select * from {{ ref('data_any_value') }}

),

data_output as (

    select * from {{ ref('data_any_value_expected') }}

),

calculate as (
    select
        key_name,
        {{ any_value('static_col') }} as static_col,
        count(id) as num_rows
    from testdata
    group by key_name
)

select
    calculate.num_rows as actual,
    data_output.num_rows as expected
from calculate
left join data_output
on calculate.key_name = data_output.key_name
and calculate.static_col = data_output.static_col
"""


exasol__models__test_any_value_yml = """
version: 2
models:
  - name: test_any_value
    tests:
      - assert_equal:
          actual: actual
          expected: expected
"""
# bool_or

exasol__seeds__data_bool_or_csv = """key_column,val1,val2
abc,1,1
abc,1,0
def,1,0
hij,1,1
hij,1,
klm,1,0
klm,1,
"""


exasol__seeds__data_bool_or_expected_csv = """key_column,val
abc,true
def,false
hij,true
klm,false
"""


exasol__models__test_bool_or_sql = """
with testdata as (

    select * from {{ ref('data_bool_or') }}

),

data_output as (

    select * from {{ ref('data_bool_or_expected') }}

),

calculate as (

    select
        key_column,
        {{ bool_or('val1 = val2') }} as val
    from testdata
    group by key_column

)

select
    calculate.val as actual,
    data_output.val as expected
from calculate
left join data_output
on calculate.key_column = data_output.key_column
"""


exasol__models__test_bool_or_yml = """
version: 2
models:
  - name: test_bool_or
    tests:
      - assert_equal:
          actual: actual
          expected: expected
"""

# cast_bool_to_text
exasol__models__test_cast_bool_to_text_sql = """
with testdata as (

    select 0=1 as inputval, 'FALSE' as expected union all
    select 1=1 as inputval, 'TRUE' as expected union all
    select null as inputval, null as expected

)

select

    {{ cast_bool_to_text("inputval") }} as actual,
    expected

from testdata
"""

exasol__models__test_cast_bool_to_text_yml = """
version: 2
models:
  - name: test_cast_bool_to_text
    tests:
      - assert_equal:
          actual: actual
          expected: expected
"""

# concat

exasol__seeds__data_concat_csv = """input_1,input_2,output_val
a,b,ab
a,,a
,b,b
,,
"""


exasol__models__test_concat_sql = """
with testdata as (

    select * from {{ ref('data_concat') }}

)

select
    {{ concat(['input_1', 'input_2']) }} as actual,
    output_val as expected

from testdata
"""


exasol__models__test_concat_yml = """
version: 2
models:
  - name: test_concat
    tests:
      - assert_equal:
          actual: actual
          expected: expected
"""

# dateadd

exasol__seeds__data_dateadd_csv = """from_time,interval_length,datepart,res
2018-01-01T01:00:00,1,day,2018-01-02T01:00:00
2018-01-01T01:00:00,1,month,2018-02-01T01:00:00
2018-01-01T01:00:00,1,year,2019-01-01T01:00:00
2018-01-01T01:00:00,1,hour,2018-01-01T02:00:00
,1,day,
"""


exasol__models__test_dateadd_sql = """
with testdata as (

    select * from {{ ref('data_dateadd') }}

)

select
    case
        when datepart = 'hour' then cast({{ dateadd('hour', 'interval_length', 'from_time') }} as {{ api.Column.translate_type('timestamp') }})
        when datepart = 'day' then cast({{ dateadd('day', 'interval_length', 'from_time') }} as {{ api.Column.translate_type('timestamp') }})
        when datepart = 'month' then cast({{ dateadd('month', 'interval_length', 'from_time') }} as {{ api.Column.translate_type('timestamp') }})
        when datepart = 'year' then cast({{ dateadd('year', 'interval_length', 'from_time') }} as {{ api.Column.translate_type('timestamp') }})
        else null
    end as actual,
    res as expected

from testdata
"""

exasol__models__test_dateadd_yml = """
version: 2
models:
  - name: test_dateadd
    tests:
      - assert_equal:
          actual: actual
          expected: expected
"""

# datediff

exasol__seeds__data_datediff_csv = """first_date,second_date,datepart,res
2018-01-01 01:00:00,2018-01-02 01:00:00,day,1
2018-01-01 01:00:00,2018-02-01 01:00:00,month,1
2018-01-01 01:00:00,2019-01-01 01:00:00,year,1
2018-01-01 01:00:00,2018-01-01 02:00:00,hour,1
2018-01-01 01:00:00,2018-01-01 02:01:00,minute,61
2018-01-01 01:00:00,2018-01-01 02:00:01,second,3601
,2018-01-01 02:00:00,hour,
2018-01-01 02:00:00,,hour,
"""


exasol__models__test_datediff_sql = """
with testdata as (

    select * from {{ ref('data_datediff') }}

)

select

    case
        when datepart = 'second' then {{ datediff('first_date', 'second_date', 'second') }}
        when datepart = 'minute' then {{ datediff('first_date', 'second_date', 'minute') }}
        when datepart = 'hour' then {{ datediff('first_date', 'second_date', 'hour') }}
        when datepart = 'day' then {{ datediff('first_date', 'second_date', 'day') }}
        when datepart = 'month' then {{ datediff('first_date', 'second_date', 'month') }}
        when datepart = 'year' then {{ datediff('first_date', 'second_date', 'year') }}
        else null
    end as actual,
    res as expected

from testdata

-- Also test correct casting of literal values.

union all select {{ datediff("'1999-12-31 23:59:59.999999'", "'2000-01-01 00:00:00.000000'", 'second') }} as actual, 1 as expected
union all select {{ datediff("'1999-12-31 23:59:59.999999'", "'2000-01-01 00:00:00.000000'", 'minute') }} as actual, 1 as expected
union all select {{ datediff("'1999-12-31 23:59:59.999999'", "'2000-01-01 00:00:00.000000'", 'hour') }} as actual, 1 as expected
union all select {{ datediff("'1999-12-31 23:59:59.999999'", "'2000-01-01 00:00:00.000000'", 'day') }} as actual, 1 as expected
union all select {{ datediff("'1999-12-31 23:59:59.999999'", "'2000-01-01 00:00:00.000000'", 'month') }} as actual, 1 as expected
union all select {{ datediff("'1999-12-31 23:59:59.999999'", "'2000-01-01 00:00:00.000000'", 'year') }} as actual, 1 as expected
"""


exasol__models__test_datediff_yml = """
version: 2
models:
  - name: test_datediff
    tests:
      - assert_equal:
          actual: actual
          expected: expected
"""

# date_trunc

exasol__seeds__data_date_trunc_csv = """updated_at,day_val,month_val
2018-01-05T12:00:00,2018-01-05,2018-01-01
,,
"""


exasol__models__test_date_trunc_sql = """
with testdata as (

    select * from {{ ref('data_date_trunc') }}

)

select
    cast({{date_trunc('day', 'updated_at') }} as date) as actual,
    day_val as expected

from testdata

union all

select
    cast({{ date_trunc('month', 'updated_at') }} as date) as actual,
    month_val as expected

from testdata
"""


exasol__models__test_date_trunc_yml = """
version: 2
models:
  - name: test_date_trunc
    tests:
      - assert_equal:
          actual: actual
          expected: expected
"""

# escape_single_quotes

exasol__models__test_escape_single_quotes_quote_sql = """
select '{{ escape_single_quotes("they're") }}' as actual, 'they''re' as expected union all
select '{{ escape_single_quotes("they are") }}' as actual, 'they are' as expected
"""


# The expected literal is 'they\'re'. The second backslash is to escape it from Python.
exasol__models__test_escape_single_quotes_backslash_sql = """
select '{{ escape_single_quotes("they're") }}' as actual, 'they\'re' as expected union all
select '{{ escape_single_quotes("they are") }}' as actual, 'they are' as expected
"""


exasol__models__test_escape_single_quotes_yml = """
version: 2
models:
  - name: test_escape_single_quotes
    tests:
      - assert_equal:
          actual: actual
          expected: expected
"""


# hash

exasol__seeds__data_hash_csv = """input_1,output_val
ab,187ef4436122d1cc2f40dc2b92f0eba0
a,0cc175b9c0f1b6a831c399e269772661
1,c4ca4238a0b923820dcc509a6f75849b
,
"""


exasol__models__test_hash_sql = """
with testdata as (

    select * from {{ ref('data_hash') }}

)

select
    {{ hash('input_1') }} as actual,
    output_val as expected

from testdata
"""


exasol__models__test_hash_yml = """
version: 2
models:
  - name: test_hash
    tests:
      - assert_equal:
          actual: actual
          expected: expected
"""

# last_day

exasol__seeds__data_last_day_csv = """date_day,date_part,res
2018-01-02,month,2018-01-31
2018-01-02,quarter,2018-03-31
2018-01-02,year,2018-12-31
,month,
"""


exasol__models__test_last_day_sql = """
with testdata as (

    select * from {{ ref('data_last_day') }}

)

select
    case
        when date_part = 'month' then {{ last_day('date_day', 'month') }}
        when date_part = 'quarter' then {{ last_day('date_day', 'quarter') }}
        when date_part = 'year' then {{ last_day('date_day', 'year') }}
        else null
    end as actual,
    res as expected

from testdata
"""


exasol__models__test_last_day_yml = """
version: 2
models:
  - name: test_last_day
    tests:
      - assert_equal:
          actual: actual
          expected: expected
"""

# length

exasol__seeds__data_length_csv = """expression,output_val
abcdef,6
fishtown,8
december,8
www.google.com/path,19
"""


exasol__models__test_length_sql = """
with testdata as (

    select * from {{ ref('data_length') }}

)

select

    {{ length('expression') }} as actual,
    output_val as expected

from testdata
"""


exasol__models__test_length_yml = """
version: 2
models:
  - name: test_length
    tests:
      - assert_equal:
          actual: actual
          expected: expected
"""

# listagg

exasol__seeds__data_listagg_csv = """group_col,string_text,order_col
1,a,1
1,b,2
1,c,3
2,a,2
2,1,1
2,p,3
3,g,1
3,g,2
3,g,3
"""


exasol__seeds__data_listagg_output_csv = """group_col,expected,version
1,"a_|_b_|_c",bottom_ordered
2,"1_|_a_|_p",bottom_ordered
3,"g_|_g_|_g",bottom_ordered
1,"a_|_b",bottom_ordered_limited
2,"1_|_a",bottom_ordered_limited
3,"g_|_g",bottom_ordered_limited
3,"g, g, g",comma_whitespace_unordered
3,"g",distinct_comma
3,"g,g,g",no_params
"""


exasol__models__test_listagg_sql = """
with testdata as (

    select * from {{ ref('data_listagg') }}

),

data_output as (

    select * from {{ ref('data_listagg_output') }}

),

calculate as (

    select
        group_col,
        {{ listagg('string_text', "'_|_'", "order by order_col") }} as actual,
        'bottom_ordered' as version
    from testdata
    group by group_col

    union all

    select
        group_col,
        {{ listagg('string_text', "'_|_'", "order by order_col", 2) }} as actual,
        'bottom_ordered_limited' as version
    from testdata
    group by group_col

    union all

    select
        group_col,
        {{ listagg('string_text', "', '") }} as actual,
        'comma_whitespace_unordered' as version
    from testdata
    where group_col = 3
    group by group_col

    union all

    select
        group_col,
        {{ listagg('DISTINCT string_text', "','") }} as actual,
        'distinct_comma' as version
    from testdata
    where group_col = 3
    group by group_col

    union all

    select
        group_col,
        {{ listagg('string_text') }} as actual,
        'no_params' as version
    from testdata
    where group_col = 3
    group by group_col

)

select
    calculate.actual,
    data_output.expected
from calculate
left join data_output
on calculate.group_col = data_output.group_col
and calculate.version = data_output.version
"""


exasol__models__test_listagg_yml = """
version: 2
models:
  - name: test_listagg
    tests:
      - assert_equal:
          actual: actual
          expected: expected
"""

# position

exasol__seeds__data_position_csv = """substring_text,string_text,res
def,abcdef,4
land,earth,0
town,fishtown,5
ember,december,4
"""


exasol__models__test_position_sql = """
with testdata as (

    select * from {{ ref('data_position') }}

)

select

    {{ position('substring_text', 'string_text') }} as actual,
    res as expected

from testdata
"""


exasol__models__test_position_yml = """
version: 2
models:
  - name: test_position
    tests:
      - assert_equal:
          actual: actual
          expected: expected
"""

# replace

exasol__seeds__data_replace_csv = """string_text,search_chars,replace_chars,res
a,a,b,b
http://google.com,http://,"",google.com
"""


exasol__models__test_replace_sql = """
with testdata as (

    select

        dr.*,
        coalesce(search_chars, '') as old_chars,
        coalesce(replace_chars, '') as new_chars

    from {{ ref('data_replace') }} dr

)

select

    {{ replace('string_text', 'old_chars', 'new_chars') }} as actual,
    res as expected

from testdata
"""


exasol__models__test_replace_yml = """
version: 2
models:
  - name: test_replace
    tests:
      - assert_equal:
          actual: actual
          expected: expected
"""

# right

exasol__seeds__data_right_csv = """string_text,length_expression,output_val
abcdef,3,def
fishtown,4,town
december,5,ember
december,0,
"""


exasol__models__test_right_sql = """
with testdata as (

    select * from {{ ref('data_right') }}

)

select

    {{ right('string_text', 'length_expression') }} as actual,
    coalesce(output_val, '') as expected

from testdata
"""


exasol__models__test_right_yml = """
version: 2
models:
  - name: test_right
    tests:
      - assert_equal:
          actual: actual
          expected: expected
"""

# safe_cast

exasol__seeds__data_safe_cast_csv = """field,output_val
abc,abc
123,123
,
"""


exasol__models__test_safe_cast_sql = """
with testdata as (

    select * from {{ ref('data_safe_cast') }}

)

select
    {{ safe_cast('field', api.Column.translate_type('string')) }} as actual,
    output_val as expected

from testdata
"""


exasol__models__test_safe_cast_yml = """
version: 2
models:
  - name: test_safe_cast
    tests:
      - assert_equal:
          actual: actual
          expected: expected
"""

# split_part

exasol__seeds__data_split_part_csv = """parts,split_on,result_1,result_2,result_3
a|b|c,|,a,b,c
1|2|3,|,1,2,3
,|,,,
"""


exasol__models__test_split_part_sql = """
with testdata as (

    select * from {{ ref('data_split_part') }}

)

select
    {{ split_part('parts', 'split_on', 1) }} as actual,
    result_1 as expected

from testdata

union all

select
    {{ split_part('parts', 'split_on', 2) }} as actual,
    result_2 as expected

from testdata

union all

select
    {{ split_part('parts', 'split_on', 3) }} as actual,
    result_3 as expected

from testdata
"""


exasol__models__test_split_part_yml = """
version: 2
models:
  - name: test_split_part
    tests:
      - assert_equal:
          actual: actual
          expected: expected
"""
