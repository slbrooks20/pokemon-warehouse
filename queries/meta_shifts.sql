/*
    Biggest meta shifts — Pokémon that gained or lost the most usage
    between consecutive months in a tier.

    Great for spotting what's trending up (new threats) or falling off.
*/

SELECT
    pokemon_name,
    tier,
    month,
    prev_month,
    usage_pct,
    prev_usage_pct,
    usage_pct_delta,
    rank,
    rank_change
FROM `pokemon-warehouse-2026.staging_intermediate.int_usage_trends`
WHERE tier = 'gen9ou'
  AND prev_month IS NOT NULL
ORDER BY ABS(usage_pct_delta) DESC
LIMIT 20;
