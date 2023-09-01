{% macro exasol__get_catalog(information_schemas, schemas) -%}
  {% for schema in schemas %}
    {{ log(schema)}}
  {% endfor %}

  {%- call statement('catalog', fetch_result=True) -%}

    WITH tabs as (

		  select
		  	'DB' as table_database,
		  	ROOT_NAME as table_schema,
		  	OBJECT_NAME as table_name,
		  	ROOT_NAME as table_owner,
		  	OBJECT_TYPE AS table_type,
		  	OBJECT_COMMENT as table_comment
		  from sys.EXA_USER_OBJECTS 
		  WHERE OBJECT_TYPE IN('TABLE', 'VIEW')
  
    ),

    cols as (

      select
          'DB' as table_database,
          column_schema as table_schema,
          column_table as table_name,
          column_name,
          column_ordinal_position as column_index,
          column_type,
          column_comment
      from sys.exa_user_columns

    )

    select tabs.table_owner as [table_owner],
		       tabs.table_type AS [table_type],
           tabs.table_comment as [table_comment],
           cols.table_database as [table_database],
           cols.table_schema as [table_schema],
           cols.table_name as [table_name],
           cols.column_name as [column_name],
           cols.column_index as [column_index],
           cols.column_type as [column_type],
           cols.column_comment as [column_comment]
    from tabs
    join cols on tabs.table_database = cols.table_database and tabs.table_schema = cols.table_schema and tabs.table_name = cols.table_name
    order by column_index
  {%- endcall -%}

  {{ return(load_result('catalog').table) }}

{%- endmacro %}