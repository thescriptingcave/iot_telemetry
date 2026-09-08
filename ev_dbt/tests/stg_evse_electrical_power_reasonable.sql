-- dbt data quality tests
-- Each test should return 0 rows when data is valid

-- Test: power_kw should be non-negative and reasonable (0-150 kW for EVSE)
select *
from {{ ref('stg_evse_electrical') }}
where power_kw < 0
   or power_kw > 150