-- marts/mart_evse_power_daily.sql

{{ config(
    materialized='table',
    partition_by=['day'],
    cluster_by=['device_id', 'asset_id']
) }}

with source as (
    select *
    from {{ ref('stg_evse_electrical') }}
),

daily_agg as (
    select
        -- Partition / grouping
        cast(timestamp as date) as day,
        device_id,
        asset_id,

        -- Power statistics
        avg(power_kw) as avg_power_kw,
        min(power_kw) as min_power_kw,
        max(power_kw) as max_power_kw,
        sum(power_kw) as total_energy_kwh,

        -- Additional metrics
        count(*) as reading_count,

        -- Power quality stats
        avg(power_factor) as avg_power_factor,
        avg(grid_frequency_hz) as avg_grid_frequency_hz,

        -- Voltage stats
        avg(voltage_v) as avg_voltage_v,
        min(voltage_v) as min_voltage_v,
        max(voltage_v) as max_voltage_v,

        -- Current stats
        avg(current_a) as avg_current_a,
        max(current_a) as max_current_a,

        -- Thermal / derate stats
        avg(temperature_cabinet_c) as avg_temperature_cabinet_c,
        max(derate_pct) as max_derate_pct

    from source
    where power_kw > 0
    group by cast(timestamp as date), device_id, asset_id
)

select *
from daily_agg