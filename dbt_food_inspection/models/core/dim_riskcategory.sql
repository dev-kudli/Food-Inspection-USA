{{ config(materialized="table") }}

select distinct
    {{
        dbt_utils.generate_surrogate_key(["risk"])
    }} as risk_sk,
    risk

from {{ref("staging_chicago")}}