-- On-time performance by carrier and month — the headline KPI table.
-- OTP uses the DOT definition: arrivals within 15 minutes of schedule,
-- over flights that actually arrived (see int_flights_enriched).

with flights as (

    select * from {{ ref('int_flights_enriched') }}

)

select
    month_start,
    carrier_code,
    carrier_name,

    count(*)                        as scheduled_flights,
    count_if(cancelled)             as cancelled_flights,
    count_if(diverted)              as diverted_flights,
    count_if(is_reportable_arrival) as completed_flights,
    count_if(is_on_time)            as on_time_flights,

    round(100.0 * count_if(is_on_time)
        / nullif(count_if(is_reportable_arrival), 0), 2) as otp_pct,
    round(100.0 * count_if(cancelled)
        / nullif(count(*), 0), 2)                        as cancellation_pct,
    round(avg(case when is_reportable_arrival then arr_delay_min end), 2)
                                                         as avg_arr_delay_min,
    round(avg(case when is_reportable_arrival then dep_delay_min end), 2)
                                                         as avg_dep_delay_min

from flights
group by month_start, carrier_code, carrier_name
