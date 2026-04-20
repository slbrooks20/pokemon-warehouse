with source as (
    select * from {{ source('raw', 'raw_moves') }}
),

staged as (
    select
        cast(json_value(payload, '$.id') as int64) as move_id,
        json_value(payload, '$.name') as move_name,
        json_value(payload, '$.type.name') as move_type,
        json_value(payload, '$.damage_class.name') as damage_class,
        cast(json_value(payload, '$.power') as int64) as power,
        cast(json_value(payload, '$.accuracy') as int64) as accuracy,
        cast(json_value(payload, '$.pp') as int64) as pp,
        cast(json_value(payload, '$.priority') as int64) as priority,
        json_value(payload, '$.target.name') as target,
        json_value(payload, '$.effect_entries[0].short_effect') as effect_short,
        ingested_at,
        source_url
    from source
)

select * from staged
