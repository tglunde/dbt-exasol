
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

{% macro exasol__list_relations_without_caching(information_schema, schema) %}
{% call statement('list_relations_without_caching', fetch_result=True) -%}
    select
      'DB' as [database],
      table_name as [name],
      table_schema as [schema],
	    table_type as table_type
    from (
		select table_name,table_schema,'table' as table_type from sys.exa_user_tables
		union
		select view_name, view_schema,'view' from sys.exa_user_views
	  )
    where table_schema = '{{ schema | upper }}'
{% endcall %}  
{{ return(load_result('list_relations_without_caching').table) }}
{% endmacro %}

{% macro exasol__list_schemas(database) %}
  {% call statement('list_schemas', fetch_result=True, auto_begin=False) -%}
    select schema_name as [schema] from exa_schemas
  {% endcall %}
  {{ return(load_result('list_schemas').table) }}
{% endmacro %}

{% macro exasol__create_schema(database_name, schema_name) -%}
  {% call statement('create_schema', fetch_result=True, auto_begin=False) -%}
    CREATE SCHEMA IF NOT EXISTS {{ schema_name | replace('"', "") }}
  {% endcall %}
{% endmacro %}

{% macro exasol__drop_schema(database_name, schema_name) -%}
  {% call statement('drop_schema') -%}
    drop schema if exists {{database_name}}.{{schema_name}} cascade
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
    ) WHERE [schema] = '{{ schema }}'
  {%- endcall %}
  {{ return(load_result('check_schema_exists').table) }}
{% endmacro %}

{% macro exasol__create_view_as(relation, sql) -%}
  create or replace view {{ relation.schema }}.{{ relation.identifier }} as 
    {{ sql }}
{% endmacro %}

{% macro exasol__rename_relation(from_relation, to_relation) -%}
  {% call statement('rename_relation') -%}
    RENAME {{ from_relation.type }} {{ from_relation.schema }}.{{ from_relation.identifier }} TO {{ to_relation.identifier }}
  {%- endcall %}
{% endmacro %}

{% macro exasol__create_table_as(temporary, relation, sql) -%}
    CREATE TABLE {{ relation.schema }}.{{ relation.identifier }} AS 
    {{ sql }}
{% endmacro %}_

{% macro exasol__current_timestamp() -%}
  current_timestamp
{%- endmacro %}

{% macro exasol__get_columns_in_relation(relation) -%}
  {% call statement('get_columns_in_relation', fetch_result=True) %}
      select
          column_name,
          column_type,
          column_maxsize,
          column_num_prec,
          column_num_scale

      from exa_user_columns
      where column_table = '{{ relation.identifier|upper }}'
        {% if relation.schema %}
        and column_schema = '{{ relation.schema|upper }}'
        {% endif %}
      order by column_ordinal_position

  {% endcall %}
  {% set table = load_result('get_columns_in_relation').table %}
  {{ return(sql_convert_columns_in_relation(table)) }}
{% endmacro %}

