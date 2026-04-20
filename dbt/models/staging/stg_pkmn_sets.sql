with source as (
    select * from {{ source('raw', 'raw_sets') }}
),

staged as (
    select
        ingested_at,
        source_url,
        payload
    from source
)

select * from staged
