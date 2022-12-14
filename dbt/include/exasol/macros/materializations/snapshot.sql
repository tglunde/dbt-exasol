{% macro exasol__build_snapshot_table(strategy, sql) %}

    select sbq.*,
        {{ strategy.scd_id }} as dbt_scd_id,
        {{ strategy.updated_at }} as dbt_updated_at,
        {{ strategy.updated_at }} as dbt_valid_from,
        nullif({{ strategy.updated_at }}, {{ strategy.updated_at }}) as dbt_valid_to
    from (
        {{ sql }}
    ) sbq

{% endmacro %}

{% macro exasol__snapshot_staging_table_inserts(strategy, source_sql, target_relation) -%}

    with snapshot_query as (

        {{ source_sql }}

    ),

    snapshotted_data as (

        select tgt.*,
            {{ strategy.unique_key }} as dbt_unique_key

        from {{ target_relation | upper }} tgt
        where dbt_valid_to is null

    ),

    source_data as (

        select snapshot_query.*,
            {{ strategy.scd_id }} as dbt_scd_id,
            {{ strategy.unique_key }} as dbt_unique_key,
            {{ strategy.updated_at }} as dbt_updated_at,
            {{ strategy.updated_at }} as dbt_valid_from,
            nullif({{ strategy.updated_at }}, {{ strategy.updated_at }}) as dbt_valid_to

        from snapshot_query
    ),

    insertions as (

        select
            'insert' as dbt_change_type,
            source_data.*

        from source_data
        left outer join snapshotted_data on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
        where snapshotted_data.dbt_unique_key is null
           or (
                snapshotted_data.dbt_unique_key is not null
            and snapshotted_data.dbt_valid_to is null
            and (
                {{ strategy.row_changed }}
            )
        )

    )

    select * from insertions

{%- endmacro %}

{% macro exasol__snapshot_staging_table_updates(strategy, source_sql, target_relation) -%}

    with snapshot_query as (

        {{ source_sql }}

    ),

    snapshotted_data as (

        select tgt.*,
            {{ strategy.unique_key }} as dbt_unique_key

        from {{ target_relation | upper }} tgt

    ),

    source_data as (

        select
            snapshot_query.*,
            {{ strategy.scd_id }} as dbt_scd_id,
            {{ strategy.unique_key }} as dbt_unique_key,
            {{ strategy.updated_at }} as dbt_updated_at,
            {{ strategy.updated_at }} as dbt_valid_from

        from snapshot_query
    ),

    updates as (

        select
            'update' as dbt_change_type,
            snapshotted_data.dbt_scd_id,
            source_data.dbt_valid_from as dbt_valid_to

        from source_data
        join snapshotted_data on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
        where snapshotted_data.dbt_valid_to is null
        and (
            {{ strategy.row_changed }}
        )

    )

    {%- if strategy.invalidate_hard_deletes -%}
    ,

    deletes as (

        select
            'delete' as dbt_change_type,
            snapshotted_data.dbt_scd_id,
            {{ snapshot_get_time() }} as dbt_valid_to

        from snapshotted_data
        left join source_data on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
        where
            snapshotted_data.dbt_valid_to is null
            and source_data.dbt_unique_key is null
    )
    {%- endif %}

    select * from updates

    {%- if strategy.invalidate_hard_deletes %}
    union all
    select * from deletes
    {%- endif %}

{%- endmacro %}

{% macro exasol__post_snapshot(staging_relation) %}
    {% do adapter.drop_relation(staging_relation) %}
{% endmacro %}

{% macro exasol__build_snapshot_staging_table(strategy, sql, target_relation) %}
    {% set tmp_relation = make_temp_relation(target_relation) %}
    {% set inserts_select = exasol__snapshot_staging_table_inserts(strategy, sql, target_relation) %}
    {% set updates_select = exasol__snapshot_staging_table_updates(strategy, sql, target_relation) %}

    {% call statement('build_snapshot_staging_relation_inserts') %}
        {{ create_table_as(True, tmp_relation, inserts_select) }}
    {% endcall %}

    {% call statement('build_snapshot_staging_relation_updates') %}
        insert into {{ tmp_relation | replace('"', '')}} (dbt_change_type, dbt_scd_id, dbt_valid_to)
        select dbt_change_type, dbt_scd_id, dbt_valid_to from (
            {{ updates_select }}
        ) dbt_sbq;
    {% endcall %}

    {% do return(tmp_relation) %}
{% endmacro %}

{% materialization snapshot, adapter='exasol' %}
  {%- set config = model['config'] -%}

  {%- set target_table = model.get('alias', model.get('name')) -%}

  {%- set strategy_name = config.get('strategy') -%}
  {%- set unique_key = config.get('unique_key') %}
  {%- set grant_config = config.get('grants') -%}

  {% if not adapter.check_schema_exists(model.database, model.schema) %}
    {% do create_schema(model.database, model.schema) %}
  {% endif %}

  {% set target_relation_exists, target_relation = get_or_create_relation(
          database=model.database,
          schema=model.schema,
          identifier=target_table,
          type='table') -%}

  {%- if not target_relation.is_table -%}
    {% do exceptions.relation_wrong_type(target_relation, 'table') %}
  {%- endif -%}


  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% set strategy_macro = strategy_dispatch(strategy_name) %}
  {% set strategy = strategy_macro(model, "snapshotted_data", "source_data", config, target_relation_exists) %}

  {% if not target_relation_exists %}

      {% set build_sql = exasol__build_snapshot_table(strategy, model['compiled_sql']) %}
      {% set final_sql = create_table_as(False, target_relation, build_sql) %}

  {% else %}

      {{ adapter.valid_snapshot_target(target_relation) }}

      {% set staging_table = exasol__build_snapshot_staging_table(strategy, sql, target_relation) %}

      -- this may no-op if the database does not require column expansion
      {% do adapter.expand_target_column_types(from_relation=staging_table,
                                               to_relation=target_relation) %}

      {% set missing_columns = adapter.get_missing_columns(staging_table, target_relation)
                                   | rejectattr('name', 'equalto', 'dbt_change_type')
                                   | rejectattr('name', 'equalto', 'DBT_CHANGE_TYPE')
                                   | rejectattr('name', 'equalto', 'dbt_unique_key')
                                   | rejectattr('name', 'equalto', 'DBT_UNIQUE_KEY')
                                   | list %}

      {% do create_columns(target_relation, missing_columns) %}

      {% set source_columns = adapter.get_columns_in_relation(staging_table)
                                   | rejectattr('name', 'equalto', 'dbt_change_type')
                                   | rejectattr('name', 'equalto', 'DBT_CHANGE_TYPE')
                                   | rejectattr('name', 'equalto', 'dbt_unique_key')
                                   | rejectattr('name', 'equalto', 'DBT_UNIQUE_KEY')
                                   | list %}

      {% set quoted_source_columns = [] %}
      {% for column in source_columns %}
        {% do quoted_source_columns.append(adapter.quote(column.name)) %}
      {% endfor %}

      {% set final_sql = exasol__snapshot_merge_sql(
            target = target_relation,
            source = staging_table,
            insert_cols = quoted_source_columns
         )
      %}

  {% endif %}

  {% call statement('main') %}
      {{ final_sql }}
  {% endcall %}

  {% set should_revoke = should_revoke(target_relation_exists, full_refresh_mode=False) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {{ adapter.commit() }}

  {% if staging_table is defined %}
      {% do post_snapshot(staging_table) %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
