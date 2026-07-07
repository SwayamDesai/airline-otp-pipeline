-- Staging: 1:1 with the raw source. Column selection is explicit so a
-- surprise column in raw can never leak downstream unreviewed.

with source as (

    select * from {{ source('raw', 'flights') }}

),

renamed as (

    select
        flight_date,
        carrier_code,
        flight_number,
        tail_number,

        origin,
        origin_city,
        origin_state,
        dest,
        dest_city,
        dest_state,
        distance_miles,

        crs_dep_time,
        dep_time,
        dep_delay_min,
        dep_delayed_15,
        taxi_out_min,
        taxi_in_min,
        crs_arr_time,
        arr_time,
        arr_delay_min,
        arr_delayed_15,
        crs_elapsed_min,
        actual_elapsed_min,
        air_time_min,

        cancelled,
        nullif(cancellation_code, '') as cancellation_code,
        diverted,

        carrier_delay_min,
        weather_delay_min,
        nas_delay_min,
        security_delay_min,
        late_aircraft_delay_min,

        year,
        month

    from source

)

select * from renamed
