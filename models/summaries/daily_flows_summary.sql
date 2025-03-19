{{
    config(
        materialized="view"
    )
}}


select
    cal.thedate
    ,coalesce(sum(av.value_at_close), 0) as assets_under_management
    ,coalesce(sum(c.gross_transaction_amount), 0) as contributions_amount
    ,coalesce(sum(w.gross_transaction_amount), 0) as withdrawals_amount
    ,coalesce(sum(c.gross_transaction_amount), 0) - coalesce(sum(w.gross_transaction_amount), 0) as net_amount
    ,coalesce(sum(f.gross_transaction_amount), 0) as fees_amount

    ,coalesce(count(c.transaction_id), 0) as contributions_count
    ,coalesce(count(w.transaction_id), 0) as withdrawals_count
    ,coalesce(count(f.transaction_id), 0) as fees_count

from {{ ref('calendar_dates') }} as cal
inner join {{ ref('account_values') }} as av
    on cal.thedate::date = av.as_of_date::date
left join {{ ref('contributions') }} as c
    on cal.thedate::date = c.transaction_date::date
left join {{ ref('withdrawals') }} as w
    on cal.thedate::date = w.transaction_date::date
left join {{ ref('fees') }} as f
    on cal.thedate::date = f.transaction_date::date


group by cal.thedate
