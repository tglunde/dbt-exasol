/*
LIST_RELATIONS_MACRO_NAME = 'list_relations_without_caching'
GET_COLUMNS_IN_RELATION_MACRO_NAME = 'get_columns_in_relation'
LIST_SCHEMAS_MACRO_NAME = 'list_schemas'
CHECK_SCHEMA_EXISTS_MACRO_NAME = 'check_schema_exists'
CREATE_SCHEMA_MACRO_NAME = 'create_schema'
DROP_SCHEMA_MACRO_NAME = 'drop_schema'
RENAME_RELATION_MACRO_NAME = 'rename_relation'
TRUNCATE_RELATION_MACRO_NAME = 'truncate_relation'
DROP_RELATION_MACRO_NAME = 'drop_relation'
ALTER_COLUMN_TYPE_MACRO_NAME = 'alter_column_type'
 */


{% macro exasol__list_relations_without_caching(schema) %}
{% call statement('list_relations_without_caching', fetch_result=True) -%}
    select
      'db' as [database],
      lower(table_name) as [name],
      lower(table_schema) as [schema],
  	  lower(table_type) as table_type
    from (
		select table_name,table_schema,'table' as table_type from sys.exa_all_tables
		union
		select view_name, view_schema,'view' from sys.exa_all_views
	  )
    where upper(table_schema) = '{{ schema |upper }}'
{% endcall %}  
{{ return(load_result('list_relations_without_caching').table) }}
{% endmacro %}

{% macro exasol__list_schemas(database) %}
  {% call statement('list_schemas', fetch_result=True, auto_begin=False) -%}
    select schema_name as [schema] from exa_schemas
  {% endcall %}
  {{ return(load_result('list_schemas').table) }}
{% endmacro %}

{% macro exasol__create_schema(relation) -%}  
  {%- call statement('create_schema') -%}
    create schema if not exists {{ relation.without_identifier() }}
  {% endcall %}
{% endmacro %}

{% macro exasol__drop_schema(relation) -%}
  {% call statement('drop_schema') -%}
    drop schema if exists {{ relation }} cascade
  {% endcall %}
{% endmacro %}

{% macro exasol__drop_relation(relation) -%}
  {% call statement('drop_relation', fetch_result=True) -%}
    drop {{ relation.type }} if exists {{ relation.schema }}.{{ relation.identifier }}
  {%- endcall %}
{% endmacro %}

{% macro exasol__check_schema_exists(database, schema) -%}
  {% call statement('check_schema_exists', fetch_result=True, auto_begin=False) -%}
    select count(*) as schema_exist from (
		select schema_name as [schema] from exa_schemas
    ) WHERE upper([schema]) = '{{ schema | upper }}'
  {%- endcall %}
  {{ return(load_result('check_schema_exists').table) }}
{% endmacro %}

{% macro exasol__create_view_as(relation, sql) -%}
CREATE OR REPLACE VIEW {{ relation.schema }}.{{ relation.identifier }} 
{{- persist_view_column_docs(relation, sql) }}
AS 
(
    {{ sql | indent(4) }}
)
{{ persist_view_relation_docs() }}
{% endmacro %}

{% macro exasol__rename_relation(from_relation, to_relation) -%}
  {% call statement('rename_relation') -%}
    RENAME {{ from_relation.type }} {{ from_relation.schema }}.{{ from_relation.identifier }} TO {{ to_relation.identifier }}
  {%- endcall %}
{% endmacro %}

{% macro exasol__create_table_as(temporary, relation, sql, language='sql') -%}
    {%- if language == 'sql' -%}
        CREATE OR REPLACE TABLE {{ relation.schema }}.{{ relation.identifier }} AS 
        {{ sql }}
    {%- elif language == 'python' -%}
      {{ py_write_table(compiled_code=compiled_code, target_relation=relation, temporary=temporary) }}
    {%- else -%}
      {% do exceptions.raise_compiler_error("exasol__create_table_as macro didn't get supported language, it got %s" % language) %}
    {%- endif -%}
{% endmacro %}

{% macro exasol__current_timestamp() -%}
  current_timestamp
{%- endmacro %}

{% macro exasol__snapshot_string_as_time(timestamp) -%}
    {%- set result = "to_timestamp('" ~ timestamp ~ "')" -%}
    {{ return(result) }}
{%- endmacro %}

{% macro exasol__truncate_relation(relation) -%}
  {% call statement('truncate_relation') -%}
    truncate table {{ relation | replace('"', '') }}
  {%- endcall %}
{% endmacro %}

{% macro exasol__get_columns_in_relation(relation) -%}
  {% call statement('get_columns_in_relation', fetch_result=True) %}
      select
          column_name,
          regexp_substr(column_type, '[A-Za-z]+', 1) as column_type,
          column_maxsize,
          column_num_prec,
          column_num_scale
      from exa_all_columns
      where upper(column_table) = '{{ relation.identifier|upper }}'
        and upper(column_schema) = '{{ relation.schema|upper }}'
      order by column_ordinal_position

  {% endcall %}
  {% set table = load_result('get_columns_in_relation').table %}
  {{ return(sql_convert_columns_in_relation(table)) }}
{% endmacro %}

{% macro exasol__get_columns_in_query(select_sql) %}
    {% call statement('get_columns_in_query', fetch_result=True, auto_begin=False) -%}
        select * from (
            {{ select_sql }}
        ) as dbt_sbq
        where false
        limit 0
    {% endcall %}

    {{ return(load_result('get_columns_in_query').table.columns | map(attribute='name') | list) }}
{% endmacro %}

{% macro exasol__alter_relation_comment(relation, relation_comment) -%}
  {%- set comment = relation_comment | replace("'", '"') %}
  COMMENT ON {{ relation.type }} {{ relation }} IS '{{ comment }}';
{% endmacro %}

{% macro get_column_comment_sql(column_name, column_dict, apply_comment=false) -%}
  {% if (column_name|upper in column_dict) -%}
    {% set matched_column = column_name|upper -%}
  {% elif (column_name|lower in column_dict) -%}
    {% set matched_column = column_name|lower -%}
  {% elif (column_name in column_dict) -%}
    {% set matched_column = column_name -%}
  {% else -%}
    {% set matched_column = None -%}
  {% endif -%}
  {% if matched_column -%}
    {% set comment = column_dict[matched_column]['description'] | replace("'", '"') -%}
  {% else -%}
    {% set comment = "" -%}
  {% endif -%}
  {{ adapter.quote(column_name) }} {{ "COMMENT" if apply_comment }} IS '{{ comment }}'
{%- endmacro %}

{% macro exasol__alter_column_comment(relation, column_dict) -%}
    {% set query_columns = get_columns_in_query(sql) %} 
    COMMENT ON {{ relation.type }} {{ relation }} (
    {% for column_name in query_columns %}
        {{ get_column_comment_sql(column_name, column_dict) }} {{- ',' if not loop.last }}
    {% endfor %}
    );
{% endmacro %}

{% macro persist_view_column_docs(relation, sql) %}
{%- if config.persist_column_docs() %}
(
  {% set query_columns = get_columns_in_query(sql) %}
  {%- for column_name in query_columns %}
      {{ get_column_comment_sql(column_name, model.columns, true) -}}{{ ',' if not loop.last -}}
  {%- endfor %}
)
{%- endif %}
{%- endmacro %}

{% macro persist_view_relation_docs() %}
{%- if config.persist_relation_docs() %}
COMMENT IS '{{ model.description }}'
{%- endif %}
{% endmacro %}

{% macro exasol__alter_column_type(relation, column_name, new_column_type) -%}
  {% call statement('alter_column_type') %}
    alter table {{ relation }} modify column {{ adapter.quote(column_name) }} {{ new_column_type }};
  {% endcall %}
{% endmacro %}
