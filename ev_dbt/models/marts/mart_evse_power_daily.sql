-- marts/mart_evse_power_daily.sql

{{ config(
    materialized='table',
    partition_by='day',
    partition_by_field='day',
    cluster_by=['device_id', 'charger_id']
) }}

with source as (
    select *
    from {{ ref('stg_evse_electrical') }}
),

daily_agg as (
    select
        day,
        device_id,
        charger_id,
        
        -- Power statistics
        avg(power_kw) as avg_power_kw,
        min(power_kw) as min_power_kw,
        max(power_kw) as max_power_kw,
        sum(power_kw * cast(1 as double)) as total_energy_kwh,
        
        -- Additional metrics
        count(*) as reading_count,
        count(distinct session_id) as distinct_sessions,
        
        -- Voltage stats
        avg(voltage_v) as avg_voltage_v,
        min(voltage_v) as min_voltage_v,
        max(voltage_v) as max_voltage_v,
        
        -- Current stats
        avg(current_a) as avg_current_a,
        
        -- State of charge stats
        avg(state_of_charge) as avg_state_of_charge,
        min(state_of_charge) as min_state_of_charge,
        max(state_of_charge) as max_state_of_charge

    from source
    where power_kw > 0
    group by day, device_id, charger_id
)

select *
from daily_agg