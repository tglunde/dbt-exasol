{% macro incremental_delete(target_relation, tmp_relation) -%}
  {%- set unique_key = config.get('unique_key') -%}

  {% if unique_key is not none %}
    delete
    from {{ target_relation }}
    where ({{ unique_key }}) in (
      select ({{ unique_key }})
      from {{ tmp_relation.schema }}.{{tmp_relation.identifier}}
  );
  {%endif%}
{%- endmacro %}

{% macro incremental_insert(tmp_relation, target_relation, unique_key=none, statement_name="main") %}
    {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
    {%- set dest_cols_csv = dest_columns | join(', ', attribute='name')  -%}

    insert into {{ target_relation }} ({{ dest_cols_csv }})
    (
       select {{ dest_cols_csv }}
       from {{ tmp_relation.schema }}.{{ tmp_relation.identifier }}
    );
{%- endmacro %}


{% materialization incremental, adapter='exasol' -%}

  {%- set unique_key = config.get('unique_key') -%}
  {%- set full_refresh_mode = (flags.FULL_REFRESH == True) -%}
  {%- set identifier = model['alias'] -%}
  {%- set target_relation = api.Relation.create(identifier=identifier, schema=schema, database=database,  type='table') -%}
  {% set existing_relation = adapter.get_relation(database=database, schema=schema, identifier = identifier) %}
  {% set tmp_relation = make_temp_relation(target_relation) %}

  -- setup
  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% if existing_relation is none %}
    {% set build_sql = create_table_as(False, target_relation, sql) %}
  {% elif existing_relation.is_view %}
    {% do adapter.drop_relation(existing_relation) %}
    {% set build_sql = create_table_as(False, target_relation, sql) %}
  {% elif full_refresh_mode %}
    {% do drop_relation(existing_relation) %}
    {% set build_sql = create_table_as(False, target_relation, sql) %}
  {% else %}
    {% do run_query(create_table_as(True, tmp_relation, sql)) %}
    {% do run_query(incremental_delete(target_relation, tmp_relation)) %}
    {% set build_sql = incremental_insert(tmp_relation, target_relation) %}
  {% endif %}

  
  {%- call statement('main') -%}
    {{ build_sql }}
  {%- endcall -%}

{% if tmp_relation is not none %}
    {% do adapter.drop_relation(tmp_relation) %}
  {% endif %}
  
  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {{ adapter.commit() }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}
{%- endmaterialization %}
