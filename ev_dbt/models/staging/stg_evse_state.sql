-- staging/stg_evse_state.sql

{{ config(
    materialized='table',
    partition_by='day',
    partition_by_field='day',
    cluster_by=['device_id']
) }}

with source as (
    select *
    from {{ source('raw', 'evse_state') }}
),

staged as (
    select
        -- Timestamp
        cast(event_ts as timestamp) as event_ts,
        
        -- Device info
        cast(device_id as varchar) as device_id,
        cast(charger_id as varchar) as charger_id,
        cast(connector_id as varchar) as connector_id,
        
        -- State info
        cast(state as varchar) as state,
        cast(status as varchar) as status,
        cast(session_id as varchar) as session_id,
        
        -- Partition columns
        cast(date(event_ts) as date) as day,
        cast(extract(hour from event_ts) as integer) as hour

    from source
)

select *
from staged