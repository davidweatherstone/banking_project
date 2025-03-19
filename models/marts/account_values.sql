{{
    config(
        materialize="incremental",
        unqiue_key=['account_id', 'as_of_date']
    )
}}

with account_values AS(
    select * from {{ source('raw', 'account_values') }}

    {% if is_incremental() %}

    where as_of_date >= (select max(as_of_date) from {{ this }})

    {% endif %}
)

select 
    account_id
    ,value_at_close::NUMERIC(12,2) as value_at_close
    ,as_of_date
from account_values