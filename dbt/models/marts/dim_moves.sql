{{
    config(materialized='table')
}}

/*
    Move dimension — one row per move with type, power, accuracy, and category.
*/

select
    {{ dbt_utils.generate_surrogate_key(['move_id']) }} as move_key,
    move_id,
    move_name,
    move_type,
    damage_class,
    power,
    accuracy,
    pp,
    priority,
    target,
    effect_short
from {{ ref('stg_pokeapi_moves') }}
