{% macro exasol__datediff(first_date, second_date, datepart) %}
    {%- set supported_dateparts = ["year", "month", "day", "hour", "minute", "second"] %}
    {%- if datepart | lower not in supported_dateparts %}
    {{ exceptions.raise_compiler_error("Unsupported `datepart`! Use one of " ~ ','.join(supported_dateparts) ~ "!") }}
    {%- endif -%}
    
    {{ datepart | lower }}s_between({{ first_date }}, {{ second_date }})

{%- endmacro %}
