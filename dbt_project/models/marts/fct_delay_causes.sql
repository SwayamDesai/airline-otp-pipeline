-- Delay minutes attributed to each DOT cause category, by carrier and month.
-- BTS populates cause columns only for flights arriving 15+ minutes late,
-- so totals here represent attributed delay, not all delay.
-- Grain: one row per carrier per month.

with flights as (

    select * from {{ ref('int_flights_enriched') }}

),

aggregated as (

    select
        month_start,
        carrier_code,
        carrier_name,

        sum(carrier_delay_min)       as carrier_delay_min,
        sum(weather_delay_min)       as weather_delay_min,
        sum(nas_delay_min)           as nas_delay_min,
        sum(security_delay_min)      as security_delay_min,
        sum(late_aircraft_delay_min) as late_aircraft_delay_min

    from flights
    group by month_start, carrier_code, carrier_name

),

with_total as (

    select
        *,
        carrier_delay_min + weather_delay_min + nas_delay_min
            + security_delay_min + late_aircraft_delay_min as total_delay_min
    from aggregated

)

select
    month_start,
    carrier_code,
    carrier_name,
    carrier_delay_min,
    weather_delay_min,
    nas_delay_min,
    security_delay_min,
    late_aircraft_delay_min,
    total_delay_min,

    round(100.0 * carrier_delay_min       / nullif(total_delay_min, 0), 1) as carrier_pct,
    round(100.0 * weather_delay_min       / nullif(total_delay_min, 0), 1) as weather_pct,
    round(100.0 * nas_delay_min           / nullif(total_delay_min, 0), 1) as nas_pct,
    round(100.0 * security_delay_min      / nullif(total_delay_min, 0), 1) as security_pct,
    round(100.0 * late_aircraft_delay_min / nullif(total_delay_min, 0), 1) as late_aircraft_pct

from with_total
