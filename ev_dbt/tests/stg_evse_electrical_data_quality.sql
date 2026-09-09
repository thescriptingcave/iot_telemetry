-- tests/stg_evse_electrical_data_quality.sql

{{ config(
    severity='warn',
    tags=['daily']
) }}

select *
from {{ ref('stg_evse_electrical') }}
where 
    power_kw < 0 or power_kw > 500
    or voltage_v < 0 or voltage_v > 1000
    or current_a < 0 or current_a > 500
    or power_factor is not null and (power_factor < 0.5 or power_factor > 1)
    or temperature_cabinet_c is not null and (temperature_cabinet_c < -40 or temperature_cabinet_c > 150)