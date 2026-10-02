/*
    stg_district
    ------------
    One row per district. Source: raw.district

    The source names its columns A1..A16. Each is renamed using the official
    dataset documentation, and cast to its real type.

    try_cast (instead of cast) on A12 and A15: district 69 (Jesenik) holds
    '?' there. try_cast turns a non-numeric value into NULL instead of
    failing the whole model. NULL (not 0) is the honest value: unknown.
    A test makes sure ONLY district 69 is affected.
*/

with source as (

    select * from {{ source('berka_raw', 'district') }}

),

renamed as (

    select
        cast(A1  as integer)            as district_id,
        trim(A2)                        as district_name,
        trim(A3)                        as region,
        cast(A4  as integer)            as population,
        cast(A5  as integer)            as municipalities_under_500,
        cast(A6  as integer)            as municipalities_500_1999,
        cast(A7  as integer)            as municipalities_2000_9999,
        cast(A8  as integer)            as municipalities_over_10000,
        cast(A9  as integer)            as number_of_cities,
        cast(A10 as decimal(5, 1))      as urban_ratio_pct,
        cast(A11 as integer)            as average_salary,
        try_cast(A12 as decimal(4, 2))  as unemployment_rate_1995,
        cast(A13 as decimal(4, 2))      as unemployment_rate_1996,
        cast(A14 as integer)            as entrepreneurs_per_1000,
        try_cast(A15 as integer)        as crimes_1995,
        cast(A16 as integer)            as crimes_1996

    from source

)

select * from renamed