-- staging/stg_humidity.sql

{{ config(
    materialized='table',
    cluster_by=['device_id']
) }}

with source as (
    select *
    from {{ source('raw', 'humidity') }}
),

staged as (
    select
        -- Timestamp
        cast(timestamp as timestamp) as timestamp,

        -- Device info
        cast(device_id as varchar) as device_id,

        -- Environmental readings
        cast(relative_humidity_pct as double) as relative_humidity_pct,
        cast(dew_point_c as double) as dew_point_c,
        cast(location as varchar) as location

    from source

    where relative_humidity_pct is not null
)

select *
from staged
where relative_humidity_pct >= 0 and relative_humidity_pct <= 100