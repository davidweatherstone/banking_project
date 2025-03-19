{% snapshot accounts_snapshot %}

{{
    config(
        target_database='banking',
        target_schema='public',
        unique_key='account_id',
        strategy='check',
        check_cols=['close_date']
    )
}}

with latest_clients as (
    select *
    from {{ source('banking', 'accounts') }}
    where load_date = (select max(load_date) from {{ source('banking', 'accounts') }})
)


select
    account_id
    ,client_id
    ,account_type
    ,create_date
    ,close_date
from latest_clients

{% endsnapshot %}