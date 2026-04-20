{{
    config(materialized='table')
}}

/*
    Tier dimension — one row per unique tier string.
    Extracts generation number and tier name from the compound key.
*/

with distinct_tiers as (
    select distinct tier
    from {{ ref('stg_smogon_usage') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['tier']) }} as tier_key,
        tier as tier_id,
        cast(regexp_extract(tier, r'gen(\d+)') as int64) as generation,
        regexp_extract(tier, r'gen\d+(.+)') as tier_name
    from distinct_tiers
)

select * from final
