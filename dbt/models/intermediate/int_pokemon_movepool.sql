{{
    config(materialized='view')
}}

/*
    Pokémon × learnable move mapping.

    Unpacks the moves_json array from stg_pokeapi_pokemon into a flat table
    of (pokemon_id, pokemon_name, move_name).
*/

with pokemon as (
    select
        pokemon_id,
        pokemon_name,
        moves_json
    from {{ ref('stg_pokeapi_pokemon') }}
),

unnested as (
    select
        p.pokemon_id,
        p.pokemon_name,
        json_value(move_entry, '$.move.name') as move_name
    from pokemon p,
    unnest(json_query_array(p.moves_json)) as move_entry
)

select
    pokemon_id,
    pokemon_name,
    move_name
from unnested
where move_name is not null
