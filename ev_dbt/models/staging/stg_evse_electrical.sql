-- staging/stg_evse_electrical.sql

{{ config(
    materialized='table',
    partition_by='day',
    partition_by_field='day',
    cluster_by=['device_id', 'charger_id']
) }}

with source as (
    select *
    from {{ source('raw', 'evse_electrical') }}
),

staged as (
    select
        -- Timestamp
        cast(event_ts as timestamp) as event_ts,
        
        -- Device info
        cast(device_id as varchar) as device_id,
        cast(charger_id as varchar) as charger_id,
        cast(connector_id as varchar) as connector_id,
        
        -- Electrical readings
        cast(power_kw as double) as power_kw,
        cast(voltage_v as double) as voltage_v,
        cast(current_a as double) as current_a,
        cast(energy_kwh_total as double) as energy_kwh_total,
        cast(state_of_charge as double) as state_of_charge,
        cast(temperature_c as double) as temperature_c,
        
        -- Status info
        cast(status as varchar) as status,
        cast(session_id as varchar) as session_id,
        cast(evse_status as varchar) as evse_status,
        
        -- Partition columns
        cast(date(event_ts) as date) as day,
        cast(extract(hour from event_ts) as integer) as hour

    from source
    
    where power_kw is not null
)

select *
from staged
where power_kw >= 0 and power_kw <= 500
  and voltage_v >= 0 and voltage_v <= 1000
  and current_a >= 0 and current_a <= 500