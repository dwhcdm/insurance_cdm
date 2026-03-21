{% snapshot snap_counterparties %}

{{
    config(
        target_schema='curated',
        unique_key='counterparty_id',
        strategy='timestamp',
        updated_at='_loaded_at',
        invalidate_hard_deletes=True,
        tags=['snapshot', 'daily']
    )
}}

/*
    Snapshot: Counterparty SCD Type 2
    Tracks changes to counterparty risk ratings, KYC status,
    and sanctioned flags over time for audit trail.
*/

SELECT
    counterparty_id,
    legal_name,
    entity_type,
    country_of_incorporation,
    country_of_domicile,
    risk_rating,
    is_pep,
    is_sanctioned,
    industry_sector,
    last_kyc_review_date,
    source_system,
    _loaded_at

FROM {{ source('raw', 'raw_counterparties') }}

{% endsnapshot %}
