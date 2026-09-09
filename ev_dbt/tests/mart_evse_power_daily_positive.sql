-- tests/mart_evse_power_daily_positive.sql

{{ config(
    severity='warn',
    tags=['daily']
) }}

-- Test that daily aggregates have reasonable values
select *
from {{ ref('mart_evse_power_daily') }}
where 
    avg_power_kw <= 0
    or total_energy_kwh <= 0
    or avg_power_kw > max_power_kw