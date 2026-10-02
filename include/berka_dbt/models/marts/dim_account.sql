/*
    dim_account
    -----------
    Type  : conformed dimension, SCD Type 1 (the source holds no history)
    Grain : one row per bank account (4,500)

    Note: the branch district is NOT stored here. It goes on the fact
    table (branch_district_key), because district is the main analysis
    axis and should be one join away from the fact.
*/

with accounts as (

    select * from {{ ref('stg_account') }}

),

frequency_lookup as (

    -- The seed: code -> business label
    -- (POPLATEK MESICNE -> Monthly, ...)
    select code, name from {{ ref('statement_frequency_lookup') }}

)

select
    -- Surrogate key: MD5 hash of the business key
    {{ dbt_utils.generate_surrogate_key(['a.account_id']) }} as account_key,

    -- Business key kept for traceability
    a.account_id,

    a.account_open_date,
    year(a.account_open_date)          as account_open_year,

    -- Original code, unchanged (traceability to the source)
    a.statement_frequency_code,

    -- Business label from the seed.
    -- coalesce: if a code has no match in the seed, show 'Unknown'
    -- instead of NULL, so the gap is visible (and a test catches it)
    coalesce(f.name, 'Unknown')        as statement_frequency_name

from accounts a

-- LEFT join: every account is kept, even if its code is missing
-- from the seed. An inner join would silently DROP that account.
left join frequency_lookup f
    on a.statement_frequency_code = f.code