-- staging/stg_evse_session_event.sql

{{ config(
    materialized='table',
    cluster_by=['device_id']
) }}

with source as (
    select *
    from {{ source('raw', 'evse_session_event') }}
),

staged as (
    select
        -- Timestamp
        cast(timestamp as timestamp) as timestamp,

        -- Site / asset info
        cast(site_id as varchar) as site_id,
        cast(asset_id as varchar) as asset_id,
        cast(connector_id as bigint) as connector_id,
        cast(device_id as varchar) as device_id,

        -- Event info
        cast(session_id as varchar) as session_id,
        cast(event_type as varchar) as event_type,
        cast(reason_code as varchar) as reason_code,
        cast(severity as varchar) as severity,

        -- Energy metrics
        cast(meter_start_kwh as double) as meter_start_kwh,
        cast(meter_end_kwh as double) as meter_end_kwh,
        cast(energy_delivered_kwh as double) as energy_delivered_kwh,
        cast(duration_s as bigint) as duration_s,
        cast(user_id_hash as varchar) as user_id_hash

    from source
)

select *
from staged