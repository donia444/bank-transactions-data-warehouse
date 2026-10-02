select
    d.year,
    count(*)             as transactions,
    sum(f.signed_amount) as net_flow
from {{ ref('fact_transaction') }} f
join {{ ref('dim_date') }} d
    on f.transaction_date_key = d.date_key
group by d.year
order by d.year