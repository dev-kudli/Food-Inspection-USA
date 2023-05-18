{{ config(materialized="table") }}

with
    distinct_facility_type as (
        select distinct
            coalesce(trim(upper(facility_type)), 'UNKNOWN') as facility_type
        from {{ ref("staging_chicago") }}
    )

select
    {{
        dbt_utils.generate_surrogate_key(
            [
                "facility_type"
            ]
        )
    }} as facility_type_sk,
    facility_type

from distinct_facility_type

{% if target.name == 'pg_test'%}
limit 1000
{% endif %}