-- staging/stg_evse_session_event.sql

{{ config(
    materialized='table',
    partition_by='day',
    partition_by_field='day',
    cluster_by=['device_id']
) }}

with source as (
    select *
    from {{ source('raw', 'evse_session_event') }}
),

staged as (
    select
        -- Timestamp
        cast(event_ts as timestamp) as event_ts,
        
        -- Device info
        cast(device_id as varchar) as device_id,
        cast(charger_id as varchar) as charger_id,
        cast(connector_id as varchar) as connector_id,
        
        -- Event info
        cast(event_type as varchar) as event_type,
        cast(session_id as varchar) as session_id,
        
        -- Energy metrics
        cast(energy_kwh as double) as energy_kwh,
        cast(duration_minutes as double) as duration_minutes,
        cast(power_kw as double) as power_kw,
        
        -- Partition columns
        cast(date(event_ts) as date) as day,
        cast(extract(hour from event_ts) as integer) as hour

    from source
)

select *
from staged