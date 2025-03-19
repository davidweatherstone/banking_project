{{ config(
    materialized="view"
)}}

select

    c.client_id
    ,c.name
    ,c.date_of_birth
    ,c.address
    ,c.create_date
    ,c.close_date

    ,CASE 
        WHEN date_of_birth::DATE IS NULL THEN 'Unknown'
        WHEN date_of_birth::DATE > CURRENT_DATE THEN 'Invalid'
        WHEN EXTRACT(YEAR FROM AGE(date_of_birth::DATE)) >= 75 THEN '75+'
        WHEN EXTRACT(YEAR FROM AGE(date_of_birth::DATE)) BETWEEN 65 AND 74 THEN '65-74'
        WHEN EXTRACT(YEAR FROM AGE(date_of_birth::DATE)) BETWEEN 55 AND 64 THEN '55-64'
        WHEN EXTRACT(YEAR FROM AGE(date_of_birth::DATE)) BETWEEN 45 AND 54 THEN '45-54'
        WHEN EXTRACT(YEAR FROM AGE(date_of_birth::DATE)) BETWEEN 35 AND 44 THEN '35-44'
        WHEN EXTRACT(YEAR FROM AGE(date_of_birth::DATE)) BETWEEN 25 AND 34 THEN '25-34'
        WHEN EXTRACT(YEAR FROM AGE(date_of_birth::DATE)) BETWEEN 18 AND 24 THEN '18-24'
        ELSE 'Under 18'
    END AS age_band

    ,sum(av.value_at_close) as latest_client_value

from {{ ref('clients_snapshot') }} as c
left join {{ ref('accounts_snapshot') }} as acc
    on c.client_id = acc.client_id
    and acc.dbt_valid_to is null
left join {{ ref('account_values') }} as av
    on acc.account_id = av.account_id
    and av.as_of_date = (select max(as_of_date) from {{ ref('account_values') }})

where c.dbt_valid_to is null

group by
    c.client_id
    ,c.name
    ,c.date_of_birth
    ,c.address
    ,c.create_date
    ,c.close_date

    ,CASE 
        WHEN date_of_birth::DATE IS NULL THEN 'Unknown'
        WHEN date_of_birth::DATE > CURRENT_DATE THEN 'Invalid'
        WHEN EXTRACT(YEAR FROM AGE(date_of_birth::DATE)) >= 75 THEN '75+'
        WHEN EXTRACT(YEAR FROM AGE(date_of_birth::DATE)) BETWEEN 65 AND 74 THEN '65-74'
        WHEN EXTRACT(YEAR FROM AGE(date_of_birth::DATE)) BETWEEN 55 AND 64 THEN '55-64'
        WHEN EXTRACT(YEAR FROM AGE(date_of_birth::DATE)) BETWEEN 45 AND 54 THEN '45-54'
        WHEN EXTRACT(YEAR FROM AGE(date_of_birth::DATE)) BETWEEN 35 AND 44 THEN '35-44'
        WHEN EXTRACT(YEAR FROM AGE(date_of_birth::DATE)) BETWEEN 25 AND 34 THEN '25-34'
        WHEN EXTRACT(YEAR FROM AGE(date_of_birth::DATE)) BETWEEN 18 AND 24 THEN '18-24'
        ELSE 'Under 18'
    END