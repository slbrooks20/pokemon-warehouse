with source as (
    select * from {{ source('raw', 'raw_teammates') }}
),

deduplicated as (
    select
        pokemon_name,
        teammate_name,
        tier,
        month,
        usage_rate,
        ingested_at
    from source
    qualify row_number() over (
        partition by pokemon_name, teammate_name, tier, month
        order by ingested_at desc
    ) = 1
),

staged as (
    select
        {{ dbt_utils.generate_surrogate_key(['pokemon_name', 'teammate_name', 'tier', 'month']) }} as teammate_id,
        pokemon_name,
        teammate_name,
        tier,
        month,
        usage_rate,
        ingested_at
    from deduplicated
)

select * from staged
