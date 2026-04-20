/*
    Best 6-Pokémon team for a given tier and month.

    Strategy: start with the #1 usage Pokémon, then greedily pick the
    teammate with the highest co-occurrence rate that isn't already on the team.

    This is a simplified greedy approach — real teambuilding considers
    type coverage, role compression, etc. But it gives a solid "meta core."
*/

-- Parameters: change these to explore different tiers/months
DECLARE target_tier STRING DEFAULT 'gen9ou';
DECLARE target_month STRING DEFAULT '2026-03';

WITH ranked_pokemon AS (
    SELECT pokemon_name, usage_pct, rank
    FROM `pokemon-warehouse-2026.staging_marts.fct_usage`
    WHERE tier = target_tier AND month = target_month
    ORDER BY rank
    LIMIT 1
),

-- Get the top pokemon's best teammates
top_teammates AS (
    SELECT
        t.teammate_name,
        t.co_occurrence_rate
    FROM `pokemon-warehouse-2026.staging_intermediate.int_teammate_pairs` t
    WHERE t.tier = target_tier
      AND t.month = target_month
      AND t.pokemon_name = (SELECT pokemon_name FROM ranked_pokemon)
    ORDER BY t.co_occurrence_rate DESC
    LIMIT 5
)

SELECT
    1 AS slot,
    rp.pokemon_name,
    rp.usage_pct,
    NULL AS co_occurrence_with_lead
FROM ranked_pokemon rp

UNION ALL

SELECT
    ROW_NUMBER() OVER (ORDER BY tt.co_occurrence_rate DESC) + 1 AS slot,
    tt.teammate_name AS pokemon_name,
    NULL AS usage_pct,
    tt.co_occurrence_rate AS co_occurrence_with_lead
FROM top_teammates tt

ORDER BY slot;
