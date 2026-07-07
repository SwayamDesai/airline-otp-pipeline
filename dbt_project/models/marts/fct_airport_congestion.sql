-- Airport congestion by month: flight volume and taxi times, combining the
-- departure and arrival perspectives of every airport.
-- Grain: one row per airport per month.

with flights as (

    select * from {{ ref('int_flights_enriched') }}

),

departures as (

    select
        origin                 as airport_code,
        any_value(origin_city) as city,
        any_value(origin_state) as state,
        month_start,
        count(*)               as departures,
        round(avg(taxi_out_min), 2) as avg_taxi_out_min

    from flights
    where not cancelled
    group by origin, month_start

),

arrivals as (

    select
        dest as airport_code,
        month_start,
        count(*) as arrivals,
        round(avg(taxi_in_min), 2) as avg_taxi_in_min

    from flights
    where not cancelled
    group by dest, month_start

)

select
    coalesce(departures.airport_code, arrivals.airport_code) as airport_code,
    departures.city,
    departures.state,
    coalesce(departures.month_start, arrivals.month_start)   as month_start,

    coalesce(departures.departures, 0) as departures,
    coalesce(arrivals.arrivals, 0)     as arrivals,
    coalesce(departures.departures, 0)
        + coalesce(arrivals.arrivals, 0) as total_movements,

    departures.avg_taxi_out_min,
    arrivals.avg_taxi_in_min

from departures
full outer join arrivals
    on departures.airport_code = arrivals.airport_code
    and departures.month_start = arrivals.month_start
