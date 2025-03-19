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

select * from account_values