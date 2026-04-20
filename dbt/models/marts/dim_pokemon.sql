{{
    config(materialized='table')
}}

/*
    Pokémon dimension — one row per Pokémon with base stats and typing.
*/

with pokemon as (
    select
        pokemon_id,
        pokemon_name,
        base_experience,
        height,
        weight,
        sort_order,
        species_name,
        types_json,
        stats_json,
        sprite_url
    from {{ ref('stg_pokeapi_pokemon') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['pokemon_id']) }} as pokemon_key,
        pokemon_id,
        pokemon_name,
        species_name,
        -- Primary and secondary types
        json_value(json_query_array(types_json)[offset(0)], '$.type.name') as type_1,
        case
            when array_length(json_query_array(types_json)) > 1
            then json_value(json_query_array(types_json)[offset(1)], '$.type.name')
        end as type_2,
        -- Base stats
        cast(json_value(json_query_array(stats_json)[offset(0)], '$.base_stat') as int64) as hp,
        cast(json_value(json_query_array(stats_json)[offset(1)], '$.base_stat') as int64) as attack,
        cast(json_value(json_query_array(stats_json)[offset(2)], '$.base_stat') as int64) as defense,
        cast(json_value(json_query_array(stats_json)[offset(3)], '$.base_stat') as int64) as sp_attack,
        cast(json_value(json_query_array(stats_json)[offset(4)], '$.base_stat') as int64) as sp_defense,
        cast(json_value(json_query_array(stats_json)[offset(5)], '$.base_stat') as int64) as speed,
        base_experience,
        height,
        weight,
        sort_order,
        sprite_url
    from pokemon
)

select
    *,
    hp + attack + defense + sp_attack + sp_defense + speed as base_stat_total
from final
