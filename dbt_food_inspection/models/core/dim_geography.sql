{{ config(materialized="table") }}

with
    distinct_address as (
        select distinct
            coalesce(trim(upper(address)), 'UNKNOWN') as address,
            coalesce(city, 'UNKNOWN') as city,
            coalesce(state, 'UNKNOWN') as state,
            replace(zip, '0', 'UNKNOWN') as zip,
            case when latitude isnull or latitude = 0 then -99999 else latitude end as latitude,
            case when longitude isnull or longitude = 0 then -99999 else longitude end as longitude
        from {{ ref("staging_chicago") }}
    )

select
    {{
        dbt_utils.generate_surrogate_key(
            [
                "address",
                "city"
            ]
        )
    }} as geo_sk,
    address,
    city,
    state,
    zip,
    latitude,
    longitude

from distinct_address

{% if target.name == 'pg_test'%}
limit 1000
{% endif %}