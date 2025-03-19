{{ config(
    materialized="view"
)}}

select

    acc.account_id
    ,acc.client_id
    ,acc.account_type
    ,acc.create_date
    ,acc.close_date
    
    ,av.value_at_close as latest_account_value

from {{ ref('accounts_snapshot') }} as acc
left join {{ ref('account_values') }} as av
    on acc.account_id = av.account_id
    and av.as_of_date = (select max(as_of_date) from {{ ref('account_values') }})
where dbt_valid_to is null