{{
    config(
        materialized='table',
        sql_header="create temp function flatten_moves(data_json string) returns array<struct<pokemon_name string, move_name string, move_usage_pct float64>> language js as r\"\"\" if (!data_json) return []; const data = JSON.parse(data_json); const results = []; for (const [pokemon, info] of Object.entries(data)) { const moves = info.Moves || {}; for (const [move, pct] of Object.entries(moves)) { results.push({pokemon_name: pokemon, move_name: move, move_usage_pct: pct}); } } return results; \"\"\";"
    )
}}

/*
    Moveset fact table.
    Grain: one row per (pokemon, move, tier, month).
    Extracts move usage percentages from the chaos JSON payloads.

    Uses a JS UDF (created via sql_header) to flatten the nested chaos JSON
    since BigQuery requires constant JSONPath expressions.
*/

with movesets as (
    select
        tier,
        month,
        to_json_string(json_query(payload, '$.data')) as data_json
    from {{ ref('stg_smogon_movesets') }}
),

flattened as (
    select
        m.tier,
        m.month,
        entry.pokemon_name,
        entry.move_name,
        entry.move_usage_pct
    from movesets m,
    unnest(flatten_moves(m.data_json)) as entry
),

dim_pokemon as (
    select pokemon_key, pokemon_name
    from {{ ref('dim_pokemon') }}
),

dim_moves as (
    select move_key, move_name
    from {{ ref('dim_moves') }}
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
    {{ dbt_utils.generate_surrogate_key(['f.pokemon_name', 'f.move_name', 'f.tier', 'f.month']) }} as moveset_key,
    dp.pokemon_key,
    dmv.move_key,
    dt.tier_key,
    dm.month_key,
    f.pokemon_name,
    f.move_name,
    f.tier,
    f.month,
    f.move_usage_pct
from flattened f
left join dim_pokemon dp
    on lower(f.pokemon_name) = lower(dp.pokemon_name)
left join dim_moves dmv
    on lower(replace(f.move_name, ' ', '-')) = lower(dmv.move_name)
left join dim_tiers dt
    on f.tier = dt.tier_id
left join dim_months dm
    on f.month = dm.month_id
