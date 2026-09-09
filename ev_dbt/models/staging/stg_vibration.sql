-- staging/stg_vibration.sql

{{ config(
    materialized='table',
    partition_by='day',
    partition_by_field='day',
    cluster_by=['device_id']
) }}

with source as (
    select *
    from {{ source('raw', 'vibration') }}
),

staged as (
    select
        -- Timestamp
        cast(event_ts as timestamp) as event_ts,
        
        -- Device info
        cast(device_id as varchar) as device_id,
        
        -- Vibration readings (X, Y, Z axes)
        cast(x_axis as double) as x_axis,
        cast(y_axis as double) as y_axis,
        cast(z_axis as double) as z_axis,
        
        -- Frequency analysis
        cast(frequency_hz as double) as frequency_hz,
        cast(amplitude_g as double) as amplitude_g,
        
        -- Device status
        cast(battery_level as double) as battery_level,
        cast(firmware_version as varchar) as firmware_version,
        
        -- Partition columns
        cast(date(event_ts) as date) as day,
        cast(extract(hour from event_ts) as integer) as hour

    from source
    
    where x_axis is not null
)

select *
from staged
where abs(x_axis) <= 10 and abs(y_axis) <= 10 and abs(z_axis) <= 10