{{
    config(materialized='view')
}}

/*
    18×18 type effectiveness matrix.

    Unpacks the damage_relations arrays from stg_pokeapi_types into a flat
    (attacking_type, defending_type, effectiveness_multiplier) table.
*/

with types as (
    select
        type_id,
        type_name,
        double_damage_to,
        half_damage_to,
        no_damage_to
    from {{ ref('stg_pokeapi_types') }}
    -- Exclude shadow/unknown/stellar types that don't participate in matchups
    where type_id <= 18
),

-- Super effective (2x)
super_effective as (
    select
        t.type_name as attacking_type,
        json_value(target, '$.name') as defending_type,
        2.0 as effectiveness
    from types t,
    unnest(json_query_array(t.double_damage_to)) as target
),

-- Not very effective (0.5x)
not_very_effective as (
    select
        t.type_name as attacking_type,
        json_value(target, '$.name') as defending_type,
        0.5 as effectiveness
    from types t,
    unnest(json_query_array(t.half_damage_to)) as target
),

-- Immune (0x)
immune as (
    select
        t.type_name as attacking_type,
        json_value(target, '$.name') as defending_type,
        0.0 as effectiveness
    from types t,
    unnest(json_query_array(t.no_damage_to)) as target
),

-- All explicit matchups
explicit_matchups as (
    select * from super_effective
    union all
    select * from not_very_effective
    union all
    select * from immune
),

-- Cross join all type pairs, then fill in neutral (1x) for anything not explicit
all_pairs as (
    select
        a.type_name as attacking_type,
        d.type_name as defending_type
    from types a
    cross join types d
),

final as (
    select
        ap.attacking_type,
        ap.defending_type,
        coalesce(em.effectiveness, 1.0) as effectiveness
    from all_pairs ap
    left join explicit_matchups em
        on ap.attacking_type = em.attacking_type
        and ap.defending_type = em.defending_type
)

select * from final
