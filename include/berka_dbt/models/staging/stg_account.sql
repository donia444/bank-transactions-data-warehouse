/*
    stg_account
    -----------
    One row per bank account, cleaned and typed. Source: raw.account

    Staging rules: rename, cast, standardise. No joins to other sources.
*/

with source as (

    -- Read through source(), never a hard-coded table name:
    -- this is what gives dbt its lineage graph
    select * from {{ source('berka_raw', 'account') }}

),

cleaned as (

    select
        -- Business key: text -> integer
        cast(account_id as integer) as account_id,

        -- Renamed to state its ROLE: the district of the account's BRANCH
        -- (a client also has a district: their home address)
        cast(district_id as integer) as branch_district_id,

        -- '930101' -> 1993-01-01 (see macros/yymmdd_to_date.sql)
        {{ yymmdd_to_date('date') }} as account_open_date,

        -- Original code kept unchanged (traceability to the source).
        -- Its label ('Monthly', ...) is added later in dim_account.
        trim(frequency) as statement_frequency_code

    from source

)

select * from cleaned