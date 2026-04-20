{{
    config(materialized='table')
}}

/*
    Ability dimension — one row per ability.
*/

select
    {{ dbt_utils.generate_surrogate_key(['ability_id']) }} as ability_key,
    ability_id,
    ability_name,
    is_main_series,
    generation,
    effect_short
from {{ ref('stg_pokeapi_abilities') }}
