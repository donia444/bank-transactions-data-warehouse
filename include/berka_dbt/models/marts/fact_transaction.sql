/*
    fact_transaction
    ----------------
    Type  : TRANSACTION fact table, atomic grain
    Grain : one row per transaction posted to an account

    Measures
        amount         additive        (always positive, as in the source)
        signed_amount  additive        (+ credit / - debit; SUM = net flow)
        balance_after  SEMI-additive   (never SUM across time)
*/

with transactions as (

    select * from {{ ref('stg_trans') }}

),

-- Needed only to find each account's branch district
accounts as (

    select account_id, branch_district_id from {{ ref('stg_account') }}

),

-- The dimensions: only the columns needed to look up each key
dim_date     as (select date_key, full_date       from {{ ref('dim_date') }}),
dim_account  as (select account_key, account_id   from {{ ref('dim_account') }}),
dim_district as (select district_key, district_id from {{ ref('dim_district') }}),
dim_profile  as (select * from {{ ref('dim_transaction_profile') }})

select
    -- Degenerate dimension: the original ID, also the grain of the table
    t.transaction_id,

    -- Foreign keys: one per dimension ----------------------------------
    d.date_key                  as transaction_date_key,
    da.account_key,
    dd.district_key             as branch_district_key,
    p.transaction_profile_key,

    -- Degenerate dimensions: useful details with no table of their own
    t.partner_bank_code,
    t.partner_account,

    -- Measures ---------------------------------------------------------
    t.amount,

    -- The sign comes from the dimension's direction column:
    -- money in = positive, money out = negative.
    -- So SUM(signed_amount) = net money flow.
    case p.direction
        when 'CREDIT' then  t.amount
        when 'DEBIT'  then -t.amount
    end                         as signed_amount,

    t.balance_after

from transactions t

-- LEFT joins everywhere: a missing match must never DROP a transaction.
-- If a key comes back NULL, the not_null tests will catch it.
left join dim_date d
    on t.transaction_date = d.full_date

left join dim_account da
    on t.account_id = da.account_id

left join accounts a
    on t.account_id = a.account_id
left join dim_district dd
    on a.branch_district_id = dd.district_id

left join dim_profile p
    on  t.transaction_type_code = p.transaction_type_code
    and t.operation_code        = p.operation_code
    and t.k_symbol_code         = p.k_symbol_code