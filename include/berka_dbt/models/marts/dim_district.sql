/*
    dim_district
    ------------
    Type  : conformed dimension, SCD Type 0 (fixed 1995-96 statistics,
            never updated)
    Grain : one row per district (77)
*/

with districts as (

    select * from {{ ref('stg_district') }}

)

select
    -- Surrogate key: an MD5 hash of the business key.
    -- Same district_id -> same key on every rebuild.
    {{ dbt_utils.generate_surrogate_key(['district_id']) }} as district_key,

    -- Business key kept for traceability back to the source
    district_id,

    district_name,
    region,
    population,
    municipalities_under_500,
    municipalities_500_1999,
    municipalities_2000_9999,
    municipalities_over_10000,
    number_of_cities,
    urban_ratio_pct,
    average_salary,
    unemployment_rate_1995,
    unemployment_rate_1996,
    entrepreneurs_per_1000,
    crimes_1995,
    crimes_1996,

    -- Derived: crimes per 1,000 inhabitants.
    -- Raw counts make big districts look dangerous just because they
    -- have more people; a rate allows a fair comparison.
    --   1000.0 (not 1000) forces decimal division instead of integer
    --   nullif(population, 0) avoids a division-by-zero error
    round(crimes_1996 * 1000.0 / nullif(population, 0), 1) as crimes_per_1000_1996

from districts