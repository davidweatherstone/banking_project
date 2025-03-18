{{
    config(
        materialize="incremental",
        unqiue_key="transaction_id"
    )
}}

with transactions AS(
    select * from {{ source('raw', 'transactions') }}

    {% if is_incremental() %}

    where transaction_date >= (select max(transaction_date) from {{ this }})

    {% endif %}
)

select * from transactions