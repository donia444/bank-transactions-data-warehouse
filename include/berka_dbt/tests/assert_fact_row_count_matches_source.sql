-- Fails (returns a row) if transactions were lost or duplicated
-- anywhere between raw.trans and fact_transaction.
select
    (select count(*) from {{ source('berka_raw', 'trans') }})     as source_rows,
    (select count(*) from {{ ref('fact_transaction') }})          as fact_rows
where (select count(*) from {{ source('berka_raw', 'trans') }})
   <> (select count(*) from {{ ref('fact_transaction') }})