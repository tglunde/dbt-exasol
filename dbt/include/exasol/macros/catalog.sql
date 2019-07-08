{% macro exasol__get_catalog(information_schemas) -%}

  {%- call statement('catalog', fetch_result=True) -%}

    with table_owners as (

		select
			'DB' as table_database,
			TABLE_SCHEMA as table_schema,
			TABLE_NAME as table_name,
			TABLE_SCHEMA as table_owner
		from sys.exa_user_tables

    ),

    tabs as (

		select
			'DB' as table_database,
			TABLE_SCHEMA as table_schema,
			TABLE_NAME as table_name,
			TABLE_SCHEMA as table_owner,
			TABLE_COMMENT
		from sys.exa_user_tables

    ),

    cols as (

        select
            'db' as table_database,
            column_schema as table_schema,
            column_table as table_name,
            column_name,
            column_ordinal_position as column_index,
            column_type,
            column_comment
        from sys.exa_user_columns

    )

    select tabs.table_owner as [table_owner],
           cols.table_database as [table_database],
           cols.table_schema as [table_schema],
           cols.table_name as [table_name],
           cols.column_name as [column_name],
           cols.column_index as [column_index],
           cols.column_type as [column_type],
           cols.column_comment as [column_comment]
    from tabs
    join cols on tabs.table_database = cols.table_database and tabs.table_schema = cols.table_schema and tabs.table_name = cols.table_name
    join table_owners on tabs.table_database = table_owners.table_database and tabs.table_schema = table_owners.table_schema and tabs.table_name = table_owners.table_name
    order by column_index

  {%- endcall -%}

  {{ return(load_result('catalog').table) }}

{%- endmacro %}