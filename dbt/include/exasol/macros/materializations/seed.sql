{% macro replace_quotes(input_string) %}
  {{ return(input_string.replace("'","''")) }}
{% endmacro %}


{% macro exasol__basic_load_csv_rows(model, batch_size, agate_table) %}
    {% set cols_sql = get_seed_column_quoted_csv(model, agate_table.column_names) %}
    {% set bindings = [] %}

    {% set statements = [] %}

    {% for chunk in agate_table.rows | batch(batch_size) %}
        {% set bindings = [] %}
        {% for row in chunk %}
          {% do bindings.extend(row) %}
        {% endfor %}

        {% set sql %}
            insert into {{ this.render() }} ({{ cols_sql }}) values
            {% for row in chunk -%}
                ({%- for column in agate_table.column_names -%}
                    {% set col_type = agate_table.columns[column].data_type | string %}
                    {%- if "text.Text" in col_type -%}
                      '{{replace_quotes(row[column]) if row[column]}}' {% else %} '{{ row[column] if row[column] }}'
                    {%- endif -%}
                    {%- if not loop.last%},{%- endif %}
                {%- endfor -%})
                {%- if not loop.last%}{{','+'\n'}}{%- endif %}
            {%- endfor %}
        {% endset %}

        {% do adapter.add_query(sql, bindings=bindings, abridge_sql_log=True) %}

        {% if loop.index0 == 0 %}
            {% do statements.append(sql) %}
        {% endif %}
    {% endfor %}

    {# Return SQL so we can render it out into the compiled files #}
    {{ return(statements[0]) }}
{% endmacro %}

{% macro exasol__load_csv_rows(model, agate_table) %}
  {{ return(exasol__basic_load_csv_rows(model, 10000, agate_table) )}}
{% endmacro %}