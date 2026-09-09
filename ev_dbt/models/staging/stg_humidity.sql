-- staging/stg_humidity.sql

{{ config(
    materialized='table',
    partition_by='day',
    partition_by_field='day',
    cluster_by=['device_id']
) }}

with source as (
    select *
    from {{ source('raw', 'humidity') }}
),

staged as (
    select
        -- Timestamp
        cast(event_ts as timestamp) as event_ts,
        
        -- Device info
        cast(device_id as varchar) as device_id,
        
        -- Environmental readings
        cast(humidity as double) as humidity,
        cast(temperature_c as double) as temperature_c,
        
        -- Device status
        cast(battery_level as double) as battery_level,
        
        -- Partition columns
        cast(date(event_ts) as date) as day,
        cast(extract(hour from event_ts) as integer) as hour

    from source
    
    where humidity is not null
)

select *
from staged
where humidity >= 0 and humidity <= 100