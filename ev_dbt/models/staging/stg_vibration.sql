-- staging/stg_vibration.sql

{{ config(
    materialized='table',
    cluster_by=['device_id']
) }}

with source as (
    select *
    from {{ source('raw', 'vibration') }}
),

staged as (
    select
        -- Timestamp
        cast(timestamp as timestamp) as timestamp,

        -- Device info
        cast(device_id as varchar) as device_id,

        -- Vibration readings
        cast(rms_acceleration as double) as rms_acceleration,
        cast(peak_frequency_hz as double) as peak_frequency_hz,
        cast(bandwidth_hz as double) as bandwidth_hz,
        cast(sensor_model as varchar) as sensor_model

    from source

    where rms_acceleration is not null
)

select *
from staged
where rms_acceleration >= 0 and rms_acceleration <= 50