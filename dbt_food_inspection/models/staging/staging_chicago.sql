{{ config(materialized="table") }}

select
    cast("Inspection ID" as integer) as inspection_id,
    cast("DBA Name" as text) as dba_name,
    cast("AKA Name" as text) as aka_name,
    cast("License #" as integer) as license_no,
    cast("Facility Type" as text) as facility_type,
    cast("Risk" as text) as risk,
    cast("Address" as text) as address,
    cast("City" as text) as city,
    cast("State" as text) as state,
    cast("Zip" as text) as zip,
    cast("Inspection Date" as date) as inspection_date,
    cast("Inspection Type" as text) as inspection_type,
    cast("Results" as text) as inspection_result,
    cast("Violations" as text) as violation,
    cast("Latitude" as numeric) as latitude,
    cast("Longitude" as numeric) as longitude

{% if target.name == 'pg_dev' %}
from {{ source("staging_dev", "staging_chicago") }}
{% else %}
from {{ source("staging_test", "staging_chicago") }}
{% endif %}

{% if target.name == 'pg_test' %}
limit 1000
{% endif %}