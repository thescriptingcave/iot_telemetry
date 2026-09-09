-- tests/stg_temperature_data_quality.sql

{{ config(
    severity='warn',
    tags=['daily']
) }}

select *
from {{ ref('stg_temperature') }}
where 
    temperature_c < -40
    or temperature_c > 85
    or humidity < 0
    or humidity > 100
    or battery_level < 0
    or battery_level > 100