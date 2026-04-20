/*
    Type coverage analysis for a team.

    Given a list of Pokémon, shows which types your team hits super-effectively
    and which types wall you (resist or are immune to all your STAB types).
*/

WITH my_team AS (
    SELECT pokemon_name
    FROM UNNEST(['Dragapult', 'Gholdengo', 'Great Tusk', 'Kingambit', 'Corviknight', 'Garganacl']) AS pokemon_name
),

team_types AS (
    SELECT DISTINCT
        dp.pokemon_name,
        dp.type_1 AS stab_type
    FROM `pokemon-warehouse-2026.staging_marts.dim_pokemon` dp
    INNER JOIN my_team t ON lower(t.pokemon_name) = lower(dp.pokemon_name)

    UNION DISTINCT

    SELECT DISTINCT
        dp.pokemon_name,
        dp.type_2 AS stab_type
    FROM `pokemon-warehouse-2026.staging_marts.dim_pokemon` dp
    INNER JOIN my_team t ON lower(t.pokemon_name) = lower(dp.pokemon_name)
    WHERE dp.type_2 IS NOT NULL
),

-- For each defending type, what's the best multiplier we can hit it with?
coverage AS (
    SELECT
        tm.defending_type,
        MAX(tm.effectiveness) AS best_effectiveness
    FROM `pokemon-warehouse-2026.staging_intermediate.int_type_matchups` tm
    INNER JOIN team_types tt ON tm.attacking_type = tt.stab_type
    GROUP BY tm.defending_type
)

SELECT
    defending_type,
    best_effectiveness,
    CASE
        WHEN best_effectiveness >= 2.0 THEN 'Super effective'
        WHEN best_effectiveness >= 1.0 THEN 'Neutral'
        WHEN best_effectiveness > 0.0 THEN 'Resisted'
        ELSE 'Immune'
    END AS coverage_status
FROM coverage
ORDER BY best_effectiveness ASC, defending_type;
