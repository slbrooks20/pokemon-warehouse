with source as (
    select * from {{ source('raw', 'raw_evolutions') }}
),

staged as (
    select
        cast(json_value(payload, '$.id') as int64) as chain_id,
        json_value(payload, '$.chain.species.name') as base_species,
        json_query(payload, '$.chain') as chain_json,
        ingested_at,
        source_url
    from source
)

select * from staged
