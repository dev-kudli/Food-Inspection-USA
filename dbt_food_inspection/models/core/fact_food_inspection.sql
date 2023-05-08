{{ config(materialized="table") }}

with
    staging as (
        select 
            inspection_id,
            inspection_date,
            coalesce(upper(trim(dba_name)), 'UNKNOWN') as dba_name,
            coalesce(license_no, -1) as license_no,
            coalesce(trim(upper(facility_type)), 'UNKNOWN') as facility_type,
            coalesce(trim(upper(address)), 'UNKNOWN') as address,
            coalesce(city, 'UNKNOWN') as city,
            coalesce(replace(zip, '0', 'UNKNOWN'), 'UNKNOWN') as zip,
            coalesce(trim(upper(inspection_type)), 'UNKNOWN') as inspection_type,
            regexp_replace(coalesce(trim(upper(inspection_result)), 'UNKNOWN'), '\sW\/\s', ' WITH ') as inspection_result

        from {{ ref("staging_chicago") }}
    ),
    dim_restaurant as (select * from {{ ref("dim_restaurant") }}),
    dim_geography as (select * from {{ ref("dim_geography") }}),
    dim_inspection_result as (select * from {{ ref("dim_inspection_result") }}),
    dim_facility_type as (select * from {{ ref("dim_facility_type") }}),
    dim_inspection_type as (select * from {{ ref("dim_inspection_type") }})

select distinct
    {{
        dbt_utils.generate_surrogate_key(
            [
                "staging.inspection_id"
            ]
        )
    }} as inspection_sk,
    dim_restaurant.restaurant_sk as restaurant_sk,
    dim_geography.geo_sk as geo_sk,
    dim_inspection_result.inspection_result_sk as inspection_result_sk,
    dim_facility_type.facility_type_sk as facility_type_sk,
    dim_inspection_type.inspection_type_sk as inspection_type_sk,
    staging.inspection_date as inspection_date,
    staging.inspection_id as inspection_id,
    staging.license_no as license_no

from staging

inner join dim_restaurant on staging.dba_name=dim_restaurant.dba_name

inner join dim_geography on 
staging.address=dim_geography.address and
staging.city=dim_geography.city and
staging.zip=dim_geography.zip

inner join dim_inspection_result on staging.inspection_result=dim_inspection_result.inspection_result

inner join dim_facility_type on staging.facility_type=dim_facility_type.facility_type

inner join dim_inspection_type on staging.inspection_type=dim_inspection_type.inspection_type

{% if target.name == 'pg_test'%}
limit 1000
{% endif %}