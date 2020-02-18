{% macro exasol__snapshot_hash_arguments(args) %}
    hash_md5({% for arg in args %}
        coalesce(cast({{ arg }} as varchar(100)), '') {% if not loop.last %} || '|' || {% endif %}
    {% endfor %})
{% endmacro %}

{% macro snapshot_check_all_get_existing_columns(node, target_exists) -%}
    {%- set query_columns = get_columns_in_query(node['injected_sql']) -%}
    {%- if not target_exists -%}
        {# no table yet -> return whatever the query does #}
        {{ return([false, query_columns]) }}
    {%- endif -%}
    {# handle any schema changes #}
    {%- set target_table = node.get('alias', node.get('name')) -%}
    {%- set target_relation = adapter.get_relation(database=node.database, schema=node.schema, identifier=target_table) -%}
    {%- set existing_cols = get_columns_in_query(node['injected_sql']) -%}
    {%- set ns = namespace() -%} {# handle for-loop scoping with a namespace #}
    {%- set ns.column_added = false -%}

    {%- set intersection = [] -%}
    {%- for col in query_columns -%}
        {%- if col in existing_cols -%}
            {%- do intersection.append(col) -%}
        {%- else -%}
            {% set ns.column_added = true %}
        {%- endif -%}
    {%- endfor -%}
    {{ return([ns.column_added, intersection]) }}
{%- endmacro %}
