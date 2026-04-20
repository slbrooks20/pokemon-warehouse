with source as (
    select * from {{ source('raw', 'raw_abilities') }}
),

staged as (
    select
        cast(json_value(payload, '$.id') as int64) as ability_id,
        json_value(payload, '$.name') as ability_name,
        cast(json_value(payload, '$.is_main_series') as bool) as is_main_series,
        json_value(payload, '$.generation.name') as generation,
        json_value(payload, '$.effect_entries[0].short_effect') as effect_short,
        json_query(payload, '$.pokemon') as pokemon_json,
        ingested_at,
        source_url
    from source
)

select * from staged
