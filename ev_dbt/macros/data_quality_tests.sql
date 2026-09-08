-- data quality test macros

{% macro test_not_null_and_positive(model, column_name) %}
    select *
    from {{ model }}
    where {{ column_name }} is null
       or {{ column_name }} <= 0
{% endmacro %}

{% macro test_column_value_at_least(model, column_name, threshold) %}
    select *
    from {{ model }}
    where {{ column_name }} is not null
      and {{ column_name }} < {{ threshold }}
{% endmacro %}

{% macro test_column_value_at_most(model, column_name, threshold) %}
    select *
    from {{ model }}
    where {{ column_name }} is not null
      and {{ column_name }} > {{ threshold }}
{% endmacro %}

{% macro test_column_values_in_range(model, column_name, min_val, max_val) %}
    select *
    from {{ model }}
    where {{ column_name }} is not null
      and ({{ column_name }} < {{ min_val }} or {{ column_name }} > {{ max_val }})
{% endmacro %}

{% macro test_no_nulls(model, column_name) %}
    select *
    from {{ model }}
    where {{ column_name }} is null
{% endmacro %}

{% macro test_column_values_nonnull_from_threshold(model, column_name, threshold) %}
    select *
    from {{ model }}
    where {{ column_name }} > {{ threshold }}
      and {{ column_name }} is null
{% endmacro %}

{% macro test_monotonic_increasing(model, column_name, order_by_column) %}
    select *
    from (
        select 
            {{ column_name }},
            lag({{ column_name }}) over (order by {{ order_by_column }}) as prev_{{ column_name }}
        from {{ model }}
    )
    where prev_{{ column_name }} is not null
      and {{ column_name }} < prev_{{ column_name }}
{% endmacro %}

{% macro test_expected_row_count(model, expected_value, tolerance) %}
    with actual_count as (
        select count(*) as cnt from {{ model }}
    )
    select *
    from actual_count
    where cnt < {{ expected_value }} * (1 - {{ tolerance }})
       or cnt > {{ expected_value }} * (1 + {{ tolerance }})
{% endmacro %}

{% macro test_no_duplicate_keys(model, key_columns) %}
    select *
    from (
        select 
            {{ key_columns }},
            count(*) as row_count
        from {{ model }}
        group by {{ key_columns }}
    )
    where row_count > 1
{% endmacro %}

{% macro test_temporal_consistency(model, timestamp_column, max_delay_minutes) %}
    select *
    from {{ model }}
    where {{ timestamp_column }} < current_timestamp - interval '{{ max_delay_minutes }}' minute
      and {{ timestamp_column }} > current_timestamp
{% endmacro %}

{% macro test_null_rate_under_threshold(model, column_name, threshold) %}
    with total as (
        select count(*) as total_count from {{ model }}
    ),
    nulls as (
        select count(*) as null_count from {{ model }} where {{ column_name }} is null
    )
    select 
        t.total_count,
        n.null_count,
        cast(n.null_count as float) / cast(t.total_count as float) as null_rate
    from total t, nulls n
    where cast(n.null_count as float) / cast(t.total_count as float) > {{ threshold }}
{% endmacro %}