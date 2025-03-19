{{
    config(
        materialize="incremental",
        unqiue_key="transaction_id"
    )
}}

select
    *
from {{ ref('stg_transactions') }}
where transaction_type = 'Fee'

{% if is_incremental() %}

and transaction_date >= (select max(transaction_date) from {{ this }})

{% endif %}