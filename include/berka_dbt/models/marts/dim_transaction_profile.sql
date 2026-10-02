/*
    dim_transaction_profile
    -----------------------
    Type  : JUNK dimension, static (SCD Type 0)
    Grain : one row per combination of (type, operation, k_symbol)
            that actually occurs in the data

    Why a junk dimension: the three codes are small lists that only make
    sense together (direction + channel + purpose). One table with one key
    replaces three tiny dimensions, three fact keys and three joins.

    Expected rows: 15 (17 raw combinations, minus 2 that merge once NULL
    and ' ' are both cleaned to 'N/A' in stg_trans).
*/

with combinations as (

    -- Step 1: every DISTINCT combination of the three codes.
    -- 1,056,320 transactions collapse into ~15 rows.
    select distinct
        transaction_type_code,
        operation_code,
        k_symbol_code
    from {{ ref('stg_trans') }}

),

-- The three seeds (code -> label)
type_lookup      as (select code, name, direction from {{ ref('transaction_type_lookup') }}),
operation_lookup as (select code, name            from {{ ref('operation_lookup') }}),
k_symbol_lookup  as (select code, name            from {{ ref('k_symbol_lookup') }})

select
    -- Step 3: ONE key built from the THREE codes together.
    -- The fact table will compute the same hash from the same three
    -- codes, so the join between them is exact.
    {{ dbt_utils.generate_surrogate_key([
        'c.transaction_type_code',
        'c.operation_code',
        'c.k_symbol_code'
    ]) }} as transaction_profile_key,

    -- Step 2: each code next to its label (code kept for traceability)
    c.transaction_type_code,
    coalesce(t.name, 'Unknown')      as transaction_type_name,

    c.operation_code,
    coalesce(o.name, 'Unknown')      as operation_name,

    c.k_symbol_code,
    coalesce(k.name, 'Unknown')      as k_symbol_name,

    -- The ONE place that decides if money comes in or goes out.
    -- The fact table will use it to compute signed_amount (+/-).
    coalesce(t.direction, 'UNKNOWN') as direction

from combinations c

-- LEFT joins: a combination must never disappear because one of
-- its codes is missing from a seed
left join type_lookup      t on c.transaction_type_code = t.code
left join operation_lookup o on c.operation_code        = o.code
left join k_symbol_lookup  k on c.k_symbol_code         = k.code