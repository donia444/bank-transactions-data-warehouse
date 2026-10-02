-- try_cast in stg_district turns non-numeric values into NULL.
-- Profiling found exactly one such district: 69 (Jesenik).
-- This test FAILS (returns rows) if any OTHER district has NULLs there,
-- which would mean a new data problem appeared in the source.
select district_id, district_name, unemployment_rate_1995, crimes_1995
from {{ ref('stg_district') }}
where (unemployment_rate_1995 is null or crimes_1995 is null)
  and district_id <> 69