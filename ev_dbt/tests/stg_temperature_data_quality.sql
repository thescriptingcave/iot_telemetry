-- tests/stg_temperature_data_quality.sql

{{ config(
    severity='warn',
    tags=['daily']
) }}

select *
from {{ ref('stg_temperature') }}
where 
    value_celsius < -40
    or value_celsius > 125
    or unit is null