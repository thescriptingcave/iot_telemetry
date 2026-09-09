-- staging/stg_evse_electrical.sql

{{ config(
    materialized='table',
    cluster_by=['device_id', 'asset_id']
) }}

with source as (
    select *
    from {{ source('raw', 'evse_electrical') }}
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

        -- Electrical readings
        cast(voltage_v as double) as voltage_v,
        cast(current_a as double) as current_a,
        cast(power_kw as double) as power_kw,
        cast(energy_kwh_total as double) as energy_kwh_total,
        cast(power_factor as double) as power_factor,
        cast(grid_frequency_hz as double) as grid_frequency_hz,
        cast(phase as varchar) as phase,
        cast(temperature_cabinet_c as double) as temperature_cabinet_c,
        cast(derate_pct as double) as derate_pct,

        -- Status info
        cast(status as varchar) as status

    from source

    where power_kw is not null
)

select *
from staged
where power_kw >= 0 and power_kw <= 500
  and voltage_v >= 0 and voltage_v <= 1000
  and current_a >= 0 and current_a <= 500