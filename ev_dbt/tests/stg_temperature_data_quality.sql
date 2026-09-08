-- Data quality tests for stg_temperature

-- Test: temperature_celsius should be in reasonable range (-40 to 125)
select *
from {{ ref('stg_temperature') }}
where temperature_c is not null
  and (temperature_c < -40 or temperature_c > 125)

-- Test: status should match temperature ranges
select *
from {{ ref('stg_temperature') }}
where (
    (status = 'CRITICAL' and temperature_c < 55) or
    (status = 'HIGH' and (temperature_c < 45 or temperature_c >= 55)) or
    (status = 'NORMAL' and temperature_c >= 45)
)

-- Test: accuracy_pct should be between 0 and 100 if present
select *
from {{ ref('stg_temperature') }}
where accuracy_pct is not null
  and (accuracy_pct < 0 or accuracy_pct > 100)