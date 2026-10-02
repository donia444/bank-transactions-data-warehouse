/*
    stg_trans
    ---------
    One row per transaction, cleaned and typed. Source: raw.trans

    Staging rules: rename, cast, standardise. No joins to other sources.
    Code labels (VKLAD -> Cash Deposit) are added later in the marts.
*/

with source as (

    select * from {{ source('berka_raw', 'trans') }}

),

cleaned as (

    select
        -- Keys ---------------------------------------------------------------
        cast(trans_id   as bigint)  as transaction_id,
        cast(account_id as integer) as account_id,

        -- '930101' -> 1993-01-01 (same macro as stg_account)
        {{ yymmdd_to_date('date') }} as transaction_date,

        -- Codes: NULL and whitespace both become 'N/A' ----------------------
        {{ clean_code('type') }}      as transaction_type_code,
        {{ clean_code('operation') }} as operation_code,
        {{ clean_code('k_symbol') }}  as k_symbol_code,

        -- Measures ------------------------------------------------------------
        -- DECIMAL, not DOUBLE: money must never suffer floating-point rounding
        cast(amount  as decimal(12, 2)) as amount,
        cast(balance as decimal(12, 2)) as balance_after,

        -- Partner details (future degenerate dimensions) ---------------------
        -- Empty bank code -> NULL: the transaction has no counterparty bank
        nullif(trim(bank), '') as partner_bank_code,

        -- Partner account '0' is not a real account number -> NULL.
        -- Going through DOUBLE then BIGINT handles both '41403269' and
        -- '41403269.0'. Stored as text: it's an identifier, never summed.
        case
            when try_cast(account as double) is null then null
            when try_cast(account as double) = 0     then null
            else cast(cast(try_cast(account as double) as bigint) as varchar)
        end as partner_account

    from source

)

select * from cleaned