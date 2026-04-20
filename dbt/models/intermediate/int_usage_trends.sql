{{
    config(materialized='view')
}}

/*
    Month-over-month usage trends.

    Computes the delta in usage_pct and rank for each (pokemon, tier) pair
    compared to the previous month.
*/

with usage as (
    select
        usage_id,
        pokemon_name,
        tier,
        month,
        usage_pct,
        rank,
        raw_count
    from {{ ref('stg_smogon_usage') }}
),

with_previous as (
    select
        *,
        lag(usage_pct) over (
            partition by pokemon_name, tier
            order by month
        ) as prev_usage_pct,
        lag(rank) over (
            partition by pokemon_name, tier
            order by month
        ) as prev_rank,
        lag(month) over (
            partition by pokemon_name, tier
            order by month
        ) as prev_month
    from usage
)

select
    usage_id,
    pokemon_name,
    tier,
    month,
    usage_pct,
    rank,
    raw_count,
    prev_month,
    prev_usage_pct,
    prev_rank,
    usage_pct - prev_usage_pct as usage_pct_delta,
    prev_rank - rank as rank_change  -- positive = improved (lower rank number is better)
from with_previous
