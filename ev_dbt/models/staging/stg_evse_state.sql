-- staging/stg_evse_state.sql

{{ config(
    materialized='table',
    cluster_by=['device_id']
) }}

with source as (
    select *
    from {{ source('raw', 'evse_state') }}
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

        -- State info
        cast(state as varchar) as state,
        cast(available as boolean) as available,
        cast(fault_active as boolean) as fault_active,
        cast(fault_code as varchar) as fault_code,
        cast(derate_pct as double) as derate_pct,
        cast(charger_temp_c as double) as charger_temp_c,
        cast(uptime_s as bigint) as uptime_s,
        cast(firmware_version as varchar) as firmware_version

    from source
)

select *
from staged