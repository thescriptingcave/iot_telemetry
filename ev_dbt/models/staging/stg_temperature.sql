-- staging/stg_temperature.sql

{{ config(
    materialized='table',
    partition_by='day',
    partition_by_field='day',
    cluster_by=['device_id']
) }}

with source as (
    select *
    from {{ source('raw', 'temperature') }}
),

staged as (
    select
        -- Timestamp
        cast(event_ts as timestamp) as event_ts,
        
        -- Device info
        cast(device_id as varchar) as device_id,
        
        -- Temperature readings
        cast(temperature_c as double) as temperature_c,
        
        -- Environmental readings
        cast(humidity as double) as humidity,
        
        -- Device status
        cast(battery_level as double) as battery_level,
        cast(signal_strength as double) as signal_strength,
        cast(firmware_version as varchar) as firmware_version,
        
        -- Partition columns
        cast(date(event_ts) as date) as day,
        cast(extract(hour from event_ts) as integer) as hour

    from source
    
    where temperature_c is not null
)

select *
from staged
where temperature_c >= -40 and temperature_c <= 85