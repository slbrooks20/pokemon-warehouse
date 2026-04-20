with source as (
    select * from {{ source('raw', 'raw_pokemon') }}
),

staged as (
    select
        cast(json_value(payload, '$.id') as int64) as pokemon_id,
        json_value(payload, '$.name') as pokemon_name,
        cast(json_value(payload, '$.base_experience') as int64) as base_experience,
        cast(json_value(payload, '$.height') as int64) as height,
        cast(json_value(payload, '$.weight') as int64) as weight,
        cast(json_value(payload, '$.order') as int64) as sort_order,
        json_value(payload, '$.species.name') as species_name,
        json_query(payload, '$.types') as types_json,
        json_query(payload, '$.stats') as stats_json,
        json_query(payload, '$.abilities') as abilities_json,
        json_query(payload, '$.moves') as moves_json,
        json_value(payload, '$.sprites.front_default') as sprite_url,
        ingested_at,
        source_url
    from source
)

select * from staged
