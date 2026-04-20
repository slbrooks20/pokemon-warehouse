{{
    config(materialized='table')
}}

/*
    Month dimension — one row per month that appears in usage data.
*/

with distinct_months as (
    select distinct month
    from {{ ref('stg_smogon_usage') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['month']) }} as month_key,
        month as month_id,
        cast(substr(month, 1, 4) as int64) as year,
        cast(substr(month, 6, 2) as int64) as month_number,
        parse_date('%Y-%m', month) as month_start_date
    from distinct_months
)

select * from final
