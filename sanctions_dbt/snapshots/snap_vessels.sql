{% snapshot snap_vessels %}

{{
    config(
        target_schema='curated',
        unique_key='vessel_id',
        strategy='timestamp',
        updated_at='_loaded_at',
        invalidate_hard_deletes=True,
        tags=['snapshot', 'daily']
    )
}}

/*
    Snapshot: Vessel SCD Type 2
    Tracks changes to vessel ownership, flag state, and flagged status.
    Important for detecting flag-hopping and ownership transfers.
*/

SELECT
    vessel_id,
    imo_number,
    vessel_name,
    vessel_type,
    flag_state,
    status,
    is_flagged,
    registered_owner,
    beneficial_owner,
    source_system,
    _loaded_at

FROM {{ source('raw', 'raw_vessels') }}

{% endsnapshot %}
