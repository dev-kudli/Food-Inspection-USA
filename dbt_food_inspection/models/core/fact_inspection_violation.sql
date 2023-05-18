{{ config(materialized="table") }}

with
    staging as (
        select 
            inspection_id,
            inspection_date,
            coalesce(upper(trim(dba_name)), 'UNKNOWN') as dba_name,
            coalesce(trim(upper(facility_type)), 'UNKNOWN') as facility_type,
            coalesce(trim(upper(address)), 'UNKNOWN') as address,
            coalesce(city, 'UNKNOWN') as city,
            replace(zip, '0', 'UNKNOWN') as zip,
            coalesce(trim(upper(inspection_type)), 'UNKNOWN') as inspection_type,
            regexp_replace(coalesce(trim(upper(inspection_result)), 'UNKNOWN'), '\sW\/\s', ' WITH ') as inspection_result,
            coalesce(trim(upper(violation)), 'NONE') as violation

        from {{ ref("staging_chicago") }}
    ),
    dim_restaurant as (select * from {{ ref("dim_restaurant") }}),
    dim_geography as (select * from {{ ref("dim_geography") }}),
    dim_inspection_result as (select * from {{ ref("dim_inspection_result") }}),
    dim_facility_type as (select * from {{ ref("dim_facility_type") }}),
    dim_inspection_type as (select * from {{ ref("dim_inspection_type") }}),
    fact_food_inspection as (select * from {{ ref("fact_food_inspection") }})
 
select distinct
    {{
        dbt_utils.generate_surrogate_key(
            [
                "fact_food_inspection.inspection_sk",
                "staging.violation"
            ]
        )
    }} as inspection_violation_sk,
    fact_food_inspection.inspection_sk as inspection_sk,
    staging.violation as violation

from staging

inner join dim_restaurant on staging.dba_name=dim_restaurant.dba_name

inner join dim_geography on 
staging.address=dim_geography.address and
staging.city=dim_geography.city

inner join dim_inspection_result on staging.inspection_result=dim_inspection_result.inspection_result

inner join dim_facility_type on staging.facility_type=dim_facility_type.facility_type

inner join dim_inspection_type on staging.inspection_type=dim_inspection_type.inspection_type

inner join fact_food_inspection on
staging.inspection_id=fact_food_inspection.inspection_id and
staging.inspection_date=fact_food_inspection.inspection_date and
dim_restaurant.restaurant_sk=fact_food_inspection.restaurant_sk and
dim_geography.geo_sk=fact_food_inspection.geo_sk

{% if target.name == 'pg_test'%}
limit 1000
{% endif %}