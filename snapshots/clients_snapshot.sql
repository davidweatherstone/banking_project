{% snapshot clients_snapshot %}

{{
    config(
        target_database='banking',
        target_schema='public',
        unique_key='client_id',
        strategy='check',
        check_cols=['name', 'address', 'close_date']
    )
}}

select
    client_id
    ,name
    ,date_of_birth
    ,address
    ,create_date
    ,close_date
from {{ source('banking', 'clients') }}

{% endsnapshot %}