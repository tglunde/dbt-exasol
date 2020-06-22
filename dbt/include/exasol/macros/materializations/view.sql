{% materialization view, adapter='exasol' -%}
    {{ return(create_or_replace_view()) }}
    
{%- endmaterialization %}