{% macro build_snapshot_table(strategy, sql) %}

    select sbq.*,
        {{ strategy.scd_id }} as dbt_scd_id,
        {{ strategy.updated_at }} as dbt_updated_at,
        {{ strategy.updated_at }} as dbt_valid_from,
        nullif({{ strategy.updated_at }}, {{ strategy.updated_at }}) as dbt_valid_to
    from (
        {{ sql }}
    ) sbq

{% endmacro %}

{% macro snapshot_staging_table_inserts(strategy, source_sql, target_relation) -%}

    with snapshot_query as (

        {{ source_sql }}

    ),

    snapshotted_data as (

        select tgt.*,
            {{ strategy.unique_key }} as dbt_unique_key

        from {{ target_relation | upper }} tgt

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

{% macro snapshot_staging_table_updates(strategy, source_sql, target_relation) -%}

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

    select * from updates

{%- endmacro %}

{% macro exasol__post_snapshot(staging_relation) %}
    {% do adapter.drop_relation(staging_relation) %}
{% endmacro %}