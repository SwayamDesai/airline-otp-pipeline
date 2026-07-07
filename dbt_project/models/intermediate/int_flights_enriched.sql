-- Enrichment layer: lookups joined, shared business flags defined ONCE here
-- so every mart uses identical definitions. Ephemeral — compiled into
-- downstream models as a CTE, no warehouse object created.

with flights as (

    select * from {{ ref('stg_flights') }}

),

carriers as (

    select * from {{ ref('carriers') }}

),

cancellation_codes as (

    select * from {{ ref('cancellation_codes') }}

),

enriched as (

    select
        flights.*,

        -- unknown carrier codes degrade gracefully to the raw code
        coalesce(carriers.carrier_name, flights.carrier_code) as carrier_name,
        cancellation_codes.cancellation_reason,

        date_trunc('month', flights.flight_date)::date as month_start,

        -- DOT convention: OTP is computed over flights that actually
        -- arrived — cancelled and diverted flights are excluded from the
        -- denominator (they are tracked as their own KPIs instead).
        (not flights.cancelled
         and not flights.diverted
         and flights.arr_delay_min is not null) as is_reportable_arrival,

        case
            when not flights.cancelled
                 and not flights.diverted
                 and flights.arr_delay_min is not null
            then not flights.arr_delayed_15
        end as is_on_time

    from flights
    left join carriers
        on flights.carrier_code = carriers.carrier_code
    left join cancellation_codes
        on flights.cancellation_code = cancellation_codes.cancellation_code

)

select * from enriched
