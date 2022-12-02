# seeds/my_seed.csv
my_seed_csv = """
ID,NAME,SOME_DATE
1,Easton,1981-05-20 06:46:51
2,Lillian,1978-09-03 18:10:33
3,Jeremiah,1982-03-11 03:59:51
4,Nolan,1976-05-06 20:21:35
""".lstrip()

# models/my_model.sql
my_model_sql = """
select * from {{ ref('my_seed') }}
union all
select null as id, null as name, null as some_date
"""

# models/my_model.yml
my_model_yml = """
version: 2
models:
  - name: my_model
    columns:
      - name: id
        tests:
          - unique
          - not_null  # this test will fail
"""
