{{
    config(materialized='view')
}}

/*
    Teammate co-occurrence rates.

    Enriches raw teammate data with both Pokémon's usage stats for context.
    A high co-occurrence rate between two low-usage Pokémon is more meaningful
    than between two universally popular ones.
*/

with teammates as (
    select
        teammate_id,
        pokemon_name,
        teammate_name,
        tier,
        month,
        usage_rate
    from {{ ref('stg_smogon_teammates') }}
),

usage as (
    select
        pokemon_name,
        tier,
        month,
        usage_pct,
        rank
    from {{ ref('stg_smogon_usage') }}
)

select
    t.teammate_id,
    t.pokemon_name,
    t.teammate_name,
    t.tier,
    t.month,
    t.usage_rate as co_occurrence_rate,
    u_pokemon.usage_pct as pokemon_usage_pct,
    u_pokemon.rank as pokemon_rank,
    u_teammate.usage_pct as teammate_usage_pct,
    u_teammate.rank as teammate_rank
from teammates t
left join usage u_pokemon
    on t.pokemon_name = u_pokemon.pokemon_name
    and t.tier = u_pokemon.tier
    and t.month = u_pokemon.month
left join usage u_teammate
    on t.teammate_name = u_teammate.pokemon_name
    and t.tier = u_teammate.tier
    and t.month = u_teammate.month
