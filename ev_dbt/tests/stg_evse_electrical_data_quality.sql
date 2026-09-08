-- Data quality tests for stg_evse_electrical
-- Tests for voltage, current, power, and energy fields

-- Test: voltage_v should be reasonable (200-600V for EVSE)
select *
from {{ ref('stg_evse_electrical') }}
where voltage_v is not null
  and (voltage_v < 200 or voltage_v > 600)

-- Test: current_a should be reasonable (0-200A for EVSE)
select *
from {{ ref('stg_evse_electrical') }}
where current_a is not null
  and (current_a < 0 or current_a > 200)

-- Test: power_kw should be reasonable (0-150 kW for EVSE)
select *
from {{ ref('stg_evse_electrical') }}
where power_kw is not null
  and (power_kw < 0 or power_kw > 150)

-- Test: energy_kwh_total should be non-negative
select *
from {{ ref('stg_evse_electrical') }}
where energy_kwh_total < 0

-- Test: voltage and current should be consistent with power
-- power = voltage * current / 1000 (for single phase)
select *
from {{ ref('stg_evse_electrical') }}
where power_kw > 0
  and abs(power_kw - (voltage_v * current_a / 1000)) > 50

-- Test: timestamp should not be in the future
select *
from {{ ref('stg_evse_electrical') }}
where event_ts > current_timestamp