-- staging/stg_temperature.sql

{{ config(
    materialized='table',
    cluster_by=['device_id']
) }}

with source as (
    select *
    from {{ source('raw', 'temperature') }}
),

staged as (
    select
        -- Timestamp
        cast(timestamp as timestamp) as timestamp,

        -- Device info
        cast(device_id as varchar) as device_id,

        -- Temperature reading
        cast(value_celsius as double) as value_celsius,
        cast(unit as varchar) as unit,
        cast(accuracy_pct as double) as accuracy_pct,

        -- Status
        cast(status as varchar) as status

    from source

    where value_celsius is not null
)

select *
from staged
where value_celsius >= -40 and value_celsius <= 125