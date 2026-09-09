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
    or state_of_charge < 0 or state_of_charge > 100