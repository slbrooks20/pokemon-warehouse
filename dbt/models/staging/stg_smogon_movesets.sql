with source as (
    select * from {{ source('raw', 'raw_movesets') }}
),

deduplicated as (
    select
        tier,
        month,
        source_url,
        payload,
        ingested_at
    from source
    qualify row_number() over (
        partition by tier, month
        order by ingested_at desc
    ) = 1
),

staged as (
    select
        {{ dbt_utils.generate_surrogate_key(['tier', 'month']) }} as moveset_file_id,
        tier,
        month,
        source_url,
        payload,
        ingested_at
    from deduplicated
)

select * from staged
