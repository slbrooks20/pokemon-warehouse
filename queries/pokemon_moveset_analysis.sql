/*
    Most popular moves for a specific Pokémon in a tier.

    Shows what the competitive community is actually running,
    along with move metadata (type, power, category).
*/

SELECT
    f.move_name,
    f.move_usage_pct,
    dm.move_type,
    dm.damage_class,
    dm.power,
    dm.accuracy
FROM `pokemon-warehouse-2026.staging_marts.fct_moveset` f
LEFT JOIN `pokemon-warehouse-2026.staging_marts.dim_moves` dm
    ON f.move_key = dm.move_key
WHERE f.pokemon_name = 'Dragapult'
  AND f.tier = 'gen9ou'
  AND f.month = '2026-03'
ORDER BY f.move_usage_pct DESC
LIMIT 10;
