-- Route-level (origin -> dest) performance by month: OTP, cancellations,
-- delays. Grain: one row per directed route per month.

with flights as (

    select * from {{ ref('int_flights_enriched') }}

)

select
    month_start,
    origin,
    any_value(origin_city)  as origin_city,
    any_value(origin_state) as origin_state,
    dest,
    any_value(dest_city)    as dest_city,
    any_value(dest_state)   as dest_state,
    any_value(distance_miles) as distance_miles,

    count(*)                        as scheduled_flights,
    count_if(cancelled)             as cancelled_flights,
    count_if(is_reportable_arrival) as completed_flights,

    round(100.0 * count_if(is_on_time)
        / nullif(count_if(is_reportable_arrival), 0), 2) as otp_pct,
    round(100.0 * count_if(cancelled)
        / nullif(count(*), 0), 2)                        as cancellation_pct,
    round(avg(case when is_reportable_arrival then arr_delay_min end), 2)
                                                         as avg_arr_delay_min

from flights
group by month_start, origin, dest
