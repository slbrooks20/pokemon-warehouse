{{
    config(materialized='table')
}}

/*
    Usage fact table.
    Grain: one row per (pokemon, tier, month).
    Conformed dimension keys for star-schema joins.
*/

with usage as (
    select
        pokemon_name,
        tier,
        month,
        usage_pct,
        rank,
        raw_count
    from {{ ref('stg_smogon_usage') }}
),

dim_pokemon as (
    select pokemon_key, pokemon_name
    from {{ ref('dim_pokemon') }}
),

dim_tiers as (
    select tier_key, tier_id
    from {{ ref('dim_tiers') }}
),

dim_months as (
    select month_key, month_id
    from {{ ref('dim_months') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['u.pokemon_name', 'u.tier', 'u.month']) }} as usage_key,
    dp.pokemon_key,
    dt.tier_key,
    dm.month_key,
    u.pokemon_name,
    u.tier,
    u.month,
    u.usage_pct,
    u.rank,
    u.raw_count
from usage u
left join dim_pokemon dp
    on lower(u.pokemon_name) = lower(dp.pokemon_name)
left join dim_tiers dt
    on u.tier = dt.tier_id
left join dim_months dm
    on u.month = dm.month_id
