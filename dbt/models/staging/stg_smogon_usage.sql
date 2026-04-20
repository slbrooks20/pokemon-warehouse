with source as (
    select * from {{ source('raw', 'raw_usage') }}
),

deduplicated as (
    select
        pokemon_name,
        tier,
        month,
        usage_pct,
        rank,
        raw_count,
        ingested_at
    from source
    qualify row_number() over (
        partition by pokemon_name, tier, month
        order by ingested_at desc
    ) = 1
),

staged as (
    select
        {{ dbt_utils.generate_surrogate_key(['pokemon_name', 'tier', 'month']) }} as usage_id,
        pokemon_name,
        tier,
        month,
        usage_pct,
        rank,
        raw_count,
        ingested_at
    from deduplicated
)

select * from staged
