{{ config(materialized="table") }}

with
    distinct_inspection_result as (
        select distinct
            regexp_replace(coalesce(trim(upper(inspection_result)), 'UNKNOWN'), '\sW\/\s', ' WITH ') as inspection_result
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

{% if target.name == 'pg_test'%}
limit 1000
{% endif %}