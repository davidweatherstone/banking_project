{{ config(
    materialized="view"
)}}

select

    {{ dbt_utils.star(
        from=ref('clients_snapshot'),
        except=["dbt_scd_id", "dbt_updated_at", "dbt_valid_from", "dbt_valid_to"]
        )
    }}

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

from {{ ref('clients_snapshot') }}
where dbt_valid_to is null