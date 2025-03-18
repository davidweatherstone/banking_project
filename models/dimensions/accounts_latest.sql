{{ config(
    materialized="view"
)}}

select

    {{ dbt_utils.star(
        from=ref('accounts_snapshot'),
        except=["dbt_scd_id", "dbt_updated_at", "dbt_valid_from", "dbt_valid_to"]
        )
    }}

from {{ ref('accounts_snapshot') }}
where dbt_valid_to is null