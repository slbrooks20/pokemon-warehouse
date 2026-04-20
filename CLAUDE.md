# Pokémon Competitive Meta Warehouse — Claude Code Context

## What this project is

A production-quality data warehouse for competitive Pokémon analysis. The output is a queryable, tested, documented star-schema warehouse in BigQuery, built with dbt, that lets you answer real competitive teambuilding questions with SQL. It also serves as a portfolio project demonstrating dimensional modelling, ELT pipelines, data quality testing, and orchestration.

This is a **data engineering project**, not a data science project. There is no ML, no notebooks, no pandas. Everything is SQL-first where possible.

---

## Tech stack — do not deviate from this

| Layer | Tool |
|---|---|
| Cloud warehouse | BigQuery (GCP free tier) |
| Transformation | dbt Core |
| Ingestion | Python (requests, google-cloud-bigquery) |
| Orchestration | Apache Airflow (Docker) |
| Language | Python 3.11+ |
| Version control | Git/GitHub |

If a task seems like it wants a different tool (Spark, Pandas, Prefect, Redshift, etc.), default to the stack above. Ask before introducing any new dependency.

---

## Project structure

```
pokemon-warehouse/
├── CLAUDE.md                  ← you are here
├── README.md
├── ingestion/
│   ├── pokeapi/
│   │   └── ingest_pokeapi.py
│   ├── smogon/
│   │   ├── ingest_smogon_usage.py
│   │   └── ingest_smogon_chaos.py
│   └── pkmn_cc/
│       └── ingest_sets.py
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml            ← gitignored, BigQuery credentials
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   └── marts/
│   ├── tests/
│   └── macros/
├── dags/
│   └── pokemon_warehouse_dag.py
├── queries/                   ← example SQL queries for README/portfolio
└── requirements.txt
```

---

## Environment

| Item | Value |
|---|---|
| Local project path | `/Users/papichulo/Desktop/individual_projects/pokemon_project` |
| GCP project ID | `pokemon-warehouse-2026` |
| GCP account | `samuelbrookspenaloza@gmail.com` |
| Python venv | `.venv` — activate with `source .venv/bin/activate` |
| BQ raw dataset | `raw` |

---

## Data sources and their quirks

### PokéAPI (https://pokeapi.co)
- Free REST API, no auth required
- Rate limit: be polite — add 0.5s delays between requests, batch where possible
- Key endpoints: `/pokemon`, `/move`, `/type`, `/ability`, `/evolution-chain`
- `/type` response contains a `damage_relations` object — this is where the 18×18 type effectiveness matrix comes from. Parse it carefully: `double_damage_to`, `half_damage_to`, `no_damage_to` (attacker perspective).
- Pagination: use the `limit` and `offset` query params, follow `next` URLs until null
- Load raw JSON responses into BigQuery as-is; do not transform during ingestion

### Smogon Stats (https://www.smogon.com/stats/)
- URL structure: `smogon.com/stats/YYYY-MM/` → directory listing of text files
- **Prefer the `chaos/` subfolder**: `smogon.com/stats/YYYY-MM/chaos/gen9ou-1695.json`
- These are rich JSON files containing full moveset distributions, items, abilities, spreads, and teammate data
- **Generations**: 1–9 (gen1ou through gen9lc)
- **Tiers**: OU, UU, RU, NU, PU, Ubers, LC
- **Rating suffixes**: OU uses `-1695`, all other tiers use `-1630`
- **Month selection**: each gen targets 12 months from its peak era (gen9=latest, gen8=2021-11..2022-10, gen7=2018-11..2019-10, etc.)
- Discover available months dynamically from the directory listing — do not hardcode dates
- 404s are expected (not all tiers exist for all gens/months) — log at debug level, don't count as failures
- The `teammates` field in chaos JSON is a dict of `{pokemon_name: usage_rate}` — this becomes `raw_teammates`

### pkmn/smogon (https://data.pkmn.cc)
- JSON API with recommended sets, EV spreads, natures, items
- Use for `dim_items` enrichment and recommended set data
- Simple fetch: `https://data.pkmn.cc/sets/gen9.json`

---

## BigQuery conventions — CRITICAL

**Free tier restrictions:**
- `insert_rows_json` (streaming inserts) is NOT allowed on the free tier — it will 403
- Always use `load_table_from_json` (batch load jobs) instead
- Pass dicts directly to the loader — do NOT call `json.dumps()` on the payload column, it causes double-encoding and NULL reads in BigQuery

**Schema for all raw tables:**
```
ingested_at  TIMESTAMP  REQUIRED
source_url   STRING     REQUIRED
payload      JSON       REQUIRED
```

**Raw table naming:** `raw_pokemon`, `raw_moves`, `raw_types`, `raw_abilities`, `raw_evolutions`, `raw_usage`, `raw_movesets`, `raw_teammates`, `raw_sets`

All raw tables go into dataset `raw`. dbt models output to `staging`, `intermediate`, `marts`.

---

## What's been completed

### Phase 1 — PokéAPI ingestion ✓

All 5 raw tables populated:

| Table | Rows |
|---|---|
| raw_pokemon | 1,350 |
| raw_moves | 937 |
| raw_abilities | 371 |
| raw_evolutions | 541 |
| raw_types | 21 |

Script at `ingestion/pokeapi/ingest_pokeapi.py` is complete and working.

Verify with:
```sql
SELECT JSON_VALUE(payload, '$.name') AS name, JSON_VALUE(payload, '$.id') AS id
FROM `pokemon-warehouse-2026.raw.raw_pokemon`
ORDER BY CAST(JSON_VALUE(payload, '$.id') AS INT64)
LIMIT 20;
```

### Phase 2 — Smogon chaos ingestion ✓

Script at `ingestion/smogon/ingest_smogon_chaos.py`. All gens 1-9, 7 tiers each, 12 months per gen era.

| Table | Rows |
|---|---|
| raw_usage | ~65k (depends on availability) |
| raw_movesets | ~250 |
| raw_teammates | ~30M |

### Phase 3 — pkmn.cc sets ingestion ✓

Script at `ingestion/pkmn_cc/ingest_sets.py`.

| Table | Rows |
|---|---|
| raw_sets | 1 |

### Phase 4 — dbt staging + intermediate + marts ✓

All 20 models, 131 tests. `dbt run && dbt test` passes.

- 9 staging models (with dedup for idempotent re-runs)
- 4 intermediate models (type matchups, movepool, usage trends, teammate pairs)
- 7 mart models (dim_pokemon, dim_moves, dim_abilities, dim_tiers, dim_months, fct_usage, fct_moveset)

---

## What's next — pick up here

### Step 5: Orchestration + docs

Write `ingestion/smogon/ingest_smogon_chaos.py`.

**URL pattern:** `https://www.smogon.com/stats/YYYY-MM/chaos/<tier>-1695.json`

**Tiers:** `gen9ou`, `gen9uu`, `gen9ru`, `gen9nu`, `gen9pu`, `gen9ubers`, `gen9lc`

**Month discovery:** Scrape `https://www.smogon.com/stats/` directory listing dynamically. Take the 12 most recent months. Do not hardcode dates.

**Tables to populate:**

`raw_usage` — one row per (pokemon, tier, month):
```
ingested_at   TIMESTAMP
tier          STRING        e.g. "gen9ou"
month         STRING        e.g. "2024-03"
pokemon_name  STRING
usage_pct     FLOAT64
rank          INT64
raw_count     INT64
```

`raw_movesets` — one row per chaos file:
```
ingested_at   TIMESTAMP
tier          STRING
month         STRING
source_url    STRING
payload       JSON          full chaos JSON blob
```

`raw_teammates` — one row per (pokemon, tier, month, teammate):
```
ingested_at   TIMESTAMP
tier          STRING
month         STRING
pokemon_name  STRING
teammate_name STRING
usage_rate    FLOAT64
```

**Chaos JSON structure:**
```json
{
  "info": { "metagame": "gen9ou", "cutoff": 1695, "number of battles": 12345 },
  "data": {
    "Garganacl": {
      "usage": 0.234,
      "Raw count": 5678,
      "Moves": { "Salt Cure": 0.89 },
      "Items": { "Leftovers": 0.45 },
      "Teammates": { "Corviknight": 0.23 },
      "Abilities": { "Purifying Salt": 0.95 }
    }
  }
}
```

### Step 2: pkmn.cc ingestion

Write `ingestion/pkmn_cc/ingest_sets.py`.
- Fetch `https://data.pkmn.cc/sets/gen9.json`
- Load into `raw_sets` (single row, full JSON blob as payload)

### Step 3: dbt staging layer

Init dbt, write staging models, schema.yml for every model.

`profiles.yml` (gitignored):
```yaml
pokemon_warehouse:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: oauth
      project: pokemon-warehouse-2026
      dataset: staging
      threads: 4
      timeout_seconds: 300
```

`packages.yml`:
```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: [">=1.0.0", "<2.0.0"]
```

Staging models needed:
- `stg_pokeapi_pokemon.sql`, `stg_pokeapi_moves.sql`, `stg_pokeapi_types.sql`
- `stg_pokeapi_abilities.sql`, `stg_pokeapi_evolutions.sql`
- `stg_smogon_usage.sql`, `stg_smogon_movesets.sql`, `stg_smogon_teammates.sql`
- `stg_pkmn_sets.sql`

### Step 4: Intermediate + mart layers

Intermediate models (in dependency order):
1. `int_type_matchups.sql` — 18×18 effectiveness matrix
2. `int_pokemon_movepool.sql` — Pokémon × learnable move
3. `int_usage_trends.sql` — month-over-month usage delta
4. `int_teammate_pairs.sql` — co-occurrence rates

Facts: `fct_usage`, `fct_moveset`
Dimensions: `dim_pokemon`, `dim_moves`, `dim_tiers`, `dim_months`, `dim_abilities`, `dim_items`

### Step 5: Orchestration + docs

Airflow DAG (Docker only — do not install Airflow locally):
- Monthly schedule
- Tasks: `ingest_smogon` → `ingest_pokeapi` → `dbt_build` → `dbt_test` → `dbt_docs`

---

## dbt conventions

**Staging:** one model per raw table, `stg_<source>_<entity>.sql`, surrogate keys via `dbt_utils.generate_surrogate_key`, no joins, schema.yml required

**Intermediate:** `int_<description>.sql`, joins/aggregations, feeds marts only

**Marts:** `fct_<fact>.sql` and `dim_<dimension>.sql`, Kimball star schema, explicit column lists (no SELECT *)

**Fact grains:**
- `fct_usage`: (pokemon_id, tier_id, month_id)
- `fct_moveset`: (pokemon_id, move_id, tier_id, month_id)

**Required tests:**
- `unique` + `not_null` on every dimension PK
- `relationships` on every fact FK
- `accepted_values` on type/tier columns
- Custom: usage pcts in `fct_usage` sum to ~600% (±10%) per (tier, month)

---

## Code quality standards

- Python: type hints on all functions, docstrings on all functions, no bare `except:`
- Handle: rate limiting (sleep), transient HTTP errors (retry with backoff), partial failures (log and continue)
- SQL: explicit column lists in marts/intermediates, CTEs over subqueries, alias every table
- No hardcoded credentials — ADC only
- Every script runnable standalone with `python3 ingestion/<path>/<script>.py`

## Scope boundaries — do not build these

- No dashboard or BI layer
- No ML models
- No real-time ingestion — batch/monthly only
- No SCD Type 2
- No dbt packages beyond `dbt-utils` and `dbt-bigquery`

## When to ask vs proceed

**Proceed:** implementation details within a model, column naming within conventions, adding extra dbt tests, adding comments/docstrings

**Ask:** new Python dependency, deviation from layer naming, raw table schema changes that break downstream, decisions affecting star schema grain
