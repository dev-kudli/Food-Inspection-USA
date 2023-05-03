{{ config(materialized="table") }}

with
    distinct_inspection_result as (
        select distinct
            coalesce(trim(upper(inspection_result)), 'UNKNOWN') as inspection_result
        from {{ ref("staging_chicago") }}
    )

select
    {{
        dbt_utils.generate_surrogate_key(
            [
                "inspection_result"
            ]
        )
    }} as inspection_result_sk,
    inspection_result

from distinct_inspection_result

{% if target.name == 'test'%}
limit 1000
{% endif %}