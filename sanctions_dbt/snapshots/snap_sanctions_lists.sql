{% snapshot snap_sanctions_lists %}

{{
    config(
        target_schema='curated',
        unique_key='entity_id',
        strategy='timestamp',
        updated_at='_loaded_at',
        invalidate_hard_deletes=True,
        tags=['snapshot', 'daily']
    )
}}

/*
    Snapshot: Sanctions Lists SCD Type 2
    Tracks additions, removals, and modifications to sanctions lists.
    Critical for regulatory audit trail and historical screening accuracy.
*/

SELECT
    entity_id,
    entity_name,
    entity_type,
    sanctions_list,
    sanctions_program,
    nationality,
    listed_date,
    delisted_date,
    last_updated,
    remarks,
    source_system,
    _loaded_at

FROM {{ source('raw', 'raw_sanctions_lists') }}

{% endsnapshot %}
