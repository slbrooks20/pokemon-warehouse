with source as (
    select * from {{ source('raw', 'raw_types') }}
),

staged as (
    select
        cast(json_value(payload, '$.id') as int64) as type_id,
        json_value(payload, '$.name') as type_name,
        json_query(payload, '$.damage_relations.double_damage_to') as double_damage_to,
        json_query(payload, '$.damage_relations.half_damage_to') as half_damage_to,
        json_query(payload, '$.damage_relations.no_damage_to') as no_damage_to,
        json_query(payload, '$.damage_relations.double_damage_from') as double_damage_from,
        json_query(payload, '$.damage_relations.half_damage_from') as half_damage_from,
        json_query(payload, '$.damage_relations.no_damage_from') as no_damage_from,
        ingested_at,
        source_url
    from source
)

select * from staged
