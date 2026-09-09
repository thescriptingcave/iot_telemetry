-- tests/stg_evse_electrical_power_reasonable.sql

{{ config(
    severity='warn',
    tags=['daily']
) }}

-- Test that power readings make sense relative to voltage and current
-- P = V * I (for DC, roughly accurate for AC power factor ~0.9)
select *
from {{ ref('stg_evse_electrical') }}
where 
    voltage_v > 0 
    and current_a > 0
    and (
        -- Power should be roughly within 10% of V*I
        power_kw > (voltage_v * current_a / 1000) * 1.1
        or power_kw < (voltage_v * current_a / 1000) * 0.9
    )