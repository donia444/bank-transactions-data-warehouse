/*
    dim_date
    --------
    Type  : conformed dimension, GENERATED (not read from any source)
    Grain : one row per calendar day, 1993-01-01 to 1998-12-31
*/

with spine as (

    -- dbt_utils.date_spine generates one row per day, in a column
    -- named date_day. end_date is EXCLUSIVE: 1999-01-01 means the
    -- last generated day is 1998-12-31 (the last transaction date).
    {{ dbt_utils.date_spine(
        datepart   = "day",
        start_date = "cast('1993-01-01' as date)",
        end_date   = "cast('1999-01-01' as date)"
    ) }}

),

calendar as (

    select
        -- Key in YYYYMMDD form, e.g. 19930101: readable and sorts by date
        cast(strftime(date_day, '%Y%m%d') as integer) as date_key,

        cast(date_day as date)                        as full_date,
        year(date_day)                                as year,
        quarter(date_day)                             as quarter,
        month(date_day)                               as month,
        monthname(date_day)                           as month_name,

        -- First day of the month: useful for monthly grouping
        cast(date_trunc('month', date_day) as date)   as month_start_date,

        dayname(date_day)                             as day_of_week_name,

        -- DuckDB's dayofweek: 0 = Sunday ... 6 = Saturday
        dayofweek(date_day) in (0, 6)                 as is_weekend

    from spine

)

select * from calendar