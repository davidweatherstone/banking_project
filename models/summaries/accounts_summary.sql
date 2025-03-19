{{
    config(
        materialized="view"
    )
}}

select

    acc.account_type
    
    ,count(acc.account_type) as count_by_account_type
    ,sum(av.value_at_close) as value_by_account_type

from {{ ref('accounts_snapshot') }} as acc
left join {{ ref('account_values') }} as av
    on acc.account_id = av.account_id
    and av.as_of_date = (select max(as_of_date) from {{ ref('account_values') }})

where dbt_valid_to is null

group by acc.account_type