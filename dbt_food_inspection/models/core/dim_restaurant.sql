{{ config(materialized="table") }}

with
    distinct_restaurant as (
        select distinct
           COALESCE(UPPER(TRIM(dba_name)), 'UNKNOWN') as dba_name
        from {{ ref("staging_chicago") }}
    )

select
    {{
        dbt_utils.generate_surrogate_key(
            [
                "dba_name"
            ]
        )
    }} as restaurant_sk,
    dba_name

from distinct_restaurant

{% if target.name == 'pg_test'%}
limit 1000
{% endif %}