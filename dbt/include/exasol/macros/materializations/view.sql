{% materialization view, adapter='exasol' -%}
    {{ create_or_replace_view() }}
{%- endmaterialization %}