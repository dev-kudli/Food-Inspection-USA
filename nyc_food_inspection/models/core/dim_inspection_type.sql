{{ config(materialized="table") }}

with
    distinct_inspection_type as (
        select distinct
            COALESCE(TRIM(UPPER(inspection_type)), 'UNKNOWN') as inspection_type
        from {{ ref("staging_chicago") }}
    )

select
    {{
        dbt_utils.generate_surrogate_key(
            [
                "inspection_type"
            ]
        )
    }} as inspection_type_sk,
    inspection_type

from distinct_inspection_type

{% if target.name == 'test'%}
limit 1000
{% endif %}