{{ config(materialized="table") }}

with
    distinct_risk as (
        select distinct
           coalesce(upper(trim(risk)), 'UNKNOWN') as risk
        from {{ ref("staging_chicago") }}
    )

select
    {{
        dbt_utils.generate_surrogate_key(
            [
                "risk"
            ]
        )
    }} as risk_sk,
    risk

from distinct_risk