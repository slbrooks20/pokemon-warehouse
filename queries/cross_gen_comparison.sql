/*
    Cross-generation comparison — which Pokémon dominate across multiple gens?

    Finds Pokémon that appear in the top 20 of OU across different generations,
    showing the true "timeless" threats.
*/

WITH top_by_gen AS (
    SELECT
        pokemon_name,
        tier,
        dt.generation,
        AVG(f.usage_pct) AS avg_usage_pct,
        MIN(f.rank) AS best_rank
    FROM `pokemon-warehouse-2026.staging_marts.fct_usage` f
    INNER JOIN `pokemon-warehouse-2026.staging_marts.dim_tiers` dt
        ON f.tier_key = dt.tier_key
    WHERE dt.tier_name = 'ou'
      AND f.rank <= 20
    GROUP BY pokemon_name, tier, dt.generation
)

SELECT
    pokemon_name,
    COUNT(DISTINCT generation) AS gens_in_top_20,
    ARRAY_AGG(STRUCT(generation, best_rank, ROUND(avg_usage_pct, 3)) ORDER BY generation) AS gen_details
FROM top_by_gen
GROUP BY pokemon_name
HAVING COUNT(DISTINCT generation) >= 2
ORDER BY gens_in_top_20 DESC, pokemon_name
LIMIT 20;
